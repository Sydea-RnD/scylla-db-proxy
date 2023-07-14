
use scylla::prepared_statement::PreparedStatement;
use tokio::sync::Semaphore;
use std::sync::Arc;
use actix_web::{
    web, 
    HttpRequest
};
use serde_json::json;
use scylla::{query::Query, Session, SessionBuilder, QueryResult};
use scylla::transport::load_balancing::{DcAwareRoundRobinPolicy, TokenAwarePolicy};
use scylla::transport::Compression;
use crate::queries::{get_statements};
use scylla::frame::value::ValueList;
use std::collections::HashMap;
use anyhow::Result;
use base64::{Engine as _, engine::general_purpose};
use scylla::Bytes;
use crate::app_config::load_ssl_scylla_config;

pub const APPLICATION_JSON: &str = "application/json";

pub fn erfn(status_code: u16, message: String, error_message: String, custom_error_message: String) -> serde_json::Value {
    json!({
        "status_code": status_code,
        "message": message.to_string(),
        "error_message": error_message.to_string(),
        "custom_error_message": custom_error_message.to_string()
    })
}

pub fn init_input(request: HttpRequest, body: web::Bytes) -> Result<serde_json::Value, serde_json::Value> {

    let injson: serde_json::Value = match serde_json::from_str(std::str::from_utf8(&body).unwrap_or("{}")) {
        Ok(v) => v,
        Err(e) => return Err(
            erfn(
                400,
                e.to_string(),
                e.to_string(),
                e.to_string()
            )
        )
    };
    let mut headers_list: Vec<_> = Vec::new();
    for (kkey, vvalue ) in request.headers().iter() {
        headers_list.push(json!(
            {
                kkey.to_string(): vvalue.to_str().ok()
            }
        ));
    }

    let querystring = request.query_string();

    let oj = json!({
        "body": injson,
        "method": request.method().to_string(),
        "path": request.path().to_string(),
        "headers": headers_list,
        "query_string": querystring,
        "uri": request.uri().to_string()
    });

    println!("{:?}",oj);

    Ok(oj)
}

pub fn decode_status_code(status_code: &serde_json::Value) -> Result<u16, u16> {

    let u64_status_code: u64 = status_code.as_u64().unwrap_or(500);

    let ret_status_code: u16 = match u16::try_from(u64_status_code) {
        Ok(n) => n,
        Err(_e) => return Err(500)
    };

	Ok(ret_status_code)
}

pub struct ScyllaService {
    pub parallelism: usize,
    pub db_session: Arc<Session>,
    pub p_queries: Arc<HashMap<String, PreparedStatement>>,
    pub p_queries_attributes: Arc<HashMap<String, HashMap<String, serde_json::Value>>>,
    pub p_runtime_queries: Arc<HashMap<String, String>>
}

fn get_paging_state_from_result(
    query_result_arc: &Arc<QueryResult>
) -> String {

    let query_result_clone = &query_result_arc.clone();

    general_purpose::STANDARD_NO_PAD.encode(query_result_clone.paging_state.as_ref().unwrap_or(&Bytes::from_static(b""))).to_string()
}

fn get_json_rows_from_result(
    query_result_arc: &Arc<QueryResult>
) -> Result<Vec<serde_json::Value>, anyhow::Error> {

    let query_result_clone = &query_result_arc.clone();

    let mut resout: Vec<serde_json::Value> = [].to_vec();

    if let Some(rows) = &query_result_clone.rows {
        for row in rows {
            let lline_s = row.columns[0].as_ref().unwrap().as_text().unwrap();
            resout.push(serde_json::from_str(lline_s)?);
        }
    }

    Ok(resout)
}

impl ScyllaService {
    pub async fn new(
        scylla_node_0: String, 
        scylla_node_1: String, 
        scylla_node_2: String, 
        scylla_user: String,
        scylla_password: String,
        scylla_parallelism: usize, 
        scylla_datacenter: String
    ) -> Self {

        println!("*** ScyllaService: \n\tConnecting to: \n\t\t{} \n\t\t{} \n\t\t{} \n\tDataCenter: \n\t\t{} \n\tParallelism: \n\t\t{}", 
            scylla_node_0, 
            scylla_node_1, 
            scylla_node_2, 
            scylla_datacenter,
            scylla_parallelism
        );

        let dc_robin = Box::new(DcAwareRoundRobinPolicy::new(scylla_datacenter.to_string()));
        let policy = Arc::new(TokenAwarePolicy::new(dc_robin));

        let ssl_conf = load_ssl_scylla_config().expect("^^^ SSL CONFIG ERROR");

        let session: Session = SessionBuilder::new()
            .known_node(scylla_node_0)
            .known_node(scylla_node_1)
            .known_node(scylla_node_2)
            .disallow_shard_aware_port(true)
            .user(scylla_user, scylla_password)
            .ssl_context(Some(ssl_conf.build()))
            .load_balancing(policy)
            .compression(Some(Compression::Lz4))
            .build()
            .await
            .expect("^^^ Scylla Session Error");

        let mut map_p_queries = HashMap::new();
        let mut map_p_queries_attributes = HashMap::new();
        let mut map_p_runtime_queries = HashMap::new();

        let queries = get_statements();

        for val in queries.as_object().unwrap() {
            let (kkey, vv) = val;
            let is_paged_query = vv["is_paged"].as_bool().unwrap();
            let is_prepared_query = vv["is_prepared"].as_bool().unwrap();
            let per_page_results = vv["per_page_results"].as_f64().unwrap();
            if is_prepared_query {
                let prepared_s: PreparedStatement = if is_paged_query {
                    let per_page_results = vv["per_page_results"].as_u64().unwrap();
                    session
                        .prepare(Query::new(vv["statement"].as_str().unwrap().to_string()).with_page_size(per_page_results.try_into().unwrap()))
                        .await
                        .expect(format!("Error Creating {} Prepared Query", kkey).as_str())
                } else {
                    session
                        .prepare(vv["statement"].as_str().unwrap().to_string())
                        .await
                        .expect(format!("Error Creating {} Prepared Query", kkey).as_str())
                };
                map_p_queries.insert(kkey.clone(), prepared_s);
            } else {
                map_p_runtime_queries.insert(kkey.clone(), vv["statement"].as_str().unwrap().to_string());
            }
            let mut map_two = HashMap::new();
            map_two.insert("is_query".to_string(), serde_json::Value::Bool(vv["is_query"].as_bool().unwrap()));
            map_two.insert("is_paged".to_string(), serde_json::Value::Bool(is_paged_query));
            map_two.insert("is_prepared".to_string(), serde_json::Value::Bool(is_prepared_query));
            map_two.insert("per_page_results".to_string(), serde_json::Value::Number(serde_json::Number::from_f64(per_page_results).unwrap()));
            map_two.insert("casting".to_string(), serde_json::Value::Object(vv["casting"].as_object().unwrap().clone()));
            map_p_queries_attributes.insert(kkey.clone(), map_two);
        }

        let db_session = Arc::new(session);

        ScyllaService {
            db_session: db_session,
            parallelism: scylla_parallelism,
            p_queries: Arc::new(map_p_queries),
            p_queries_attributes: Arc::new(map_p_queries_attributes),
            p_runtime_queries: Arc::new(map_p_runtime_queries)
        }
    }

    pub async fn prepare_statement(
        &self, 
        i_statement: &str
    ) -> Arc<PreparedStatement> {
        let session = self.db_session.clone();
        let ret_statement = session
            .prepare(i_statement)
            .await
            .expect("Error Creating Prepared Query");
        Arc::new(ret_statement)
    }

    pub async fn direct_statement(
        &self, 
        i_statement: String,
        per_page_results: u64,
        i_paging_state: String
    ) -> Result<serde_json::Value, anyhow::Error> {

        let session = self.db_session.clone();

        let result_arc = if i_paging_state == "".to_string() {
            if per_page_results > 0 {
                Arc::new(session.query(
                    Query::new(&i_statement).with_page_size(per_page_results.try_into().unwrap()),
                    &[],
                )
                .await?)
            } else {
                Arc::new(session.query(
                    Query::new(&i_statement),
                    &[],
                )
                .await?)
            }
        } else {
            let paging_state: scylla::Bytes = Bytes::from(general_purpose::STANDARD_NO_PAD.decode(i_paging_state).unwrap_or(b"".to_vec()));
            if per_page_results > 0 {
                Arc::new(session.query_paged(
                    Query::new(&i_statement).with_page_size(per_page_results.try_into().unwrap()),
                    &[],
                    Some(paging_state)
                )
                .await?)
            } else {
                Arc::new(session.query_paged(
                    Query::new(&i_statement),
                    &[],
                    Some(paging_state)
                )
                .await?)
            }
        };

        let o_page = get_paging_state_from_result(&result_arc);
        let resout = get_json_rows_from_result(&result_arc)?;

        Ok(
            json!(
                {
                    "records": resout,
                    "records_number": resout.len(),
                    "paging_state": o_page
                }
            )
        )
    }

    pub async fn cql_statement(
        &self, 
        i_prepared_statement: String, 
        i_substitutions: impl ValueList,
        i_paging_state: String,
    ) -> Result<serde_json::Value, anyhow::Error> {

        let queries_metadata = self.p_queries_attributes.clone();

        let result: serde_json::Value = if queries_metadata[&i_prepared_statement]["is_query"].as_bool().unwrap_or(false) {
            self.cql_query(
                i_prepared_statement.clone(), &i_substitutions, i_paging_state, queries_metadata[&i_prepared_statement]["is_prepared"].as_bool().unwrap_or(true), queries_metadata[&i_prepared_statement]["per_page_results"].as_u64().unwrap_or(0)).await?
        } else {
            self.cql_delupsert(i_prepared_statement.clone(), &i_substitutions, queries_metadata[&i_prepared_statement]["is_prepared"].as_bool().unwrap_or(true)).await?
        };

        Ok(result)
    }

    pub async fn cql_query(
        &self, 
        i_prepared_statement: String, 
        i_substitutions: impl ValueList,
        i_paging_state: String,
        is_prepared: bool,
        per_page_results: u64
    ) -> Result<serde_json::Value, anyhow::Error> {

        let session = self.db_session.clone();
        let pepared_to_execute = self.p_queries.clone();
        let runtime_to_execute = self.p_runtime_queries.clone();

        let result_arc = if i_paging_state == "".to_string() {
            if is_prepared {
                Arc::new(session.execute(
                    &pepared_to_execute[&i_prepared_statement],
                    i_substitutions,
                )
                .await?)
            } else if per_page_results > 0 {
                Arc::new(session.query(
                    Query::new(&runtime_to_execute[&i_prepared_statement]).with_page_size(per_page_results.try_into().unwrap()),
                    i_substitutions,
                )
                .await?)
            } else {
                Arc::new(session.query(
                    Query::new(&runtime_to_execute[&i_prepared_statement]),
                    i_substitutions,
                )
                .await?)
            }
        } else {
            let paging_state: scylla::Bytes = Bytes::from(general_purpose::STANDARD_NO_PAD.decode(i_paging_state).unwrap_or(b"".to_vec()));
            if is_prepared {
                Arc::new(session.execute_paged(
                    &pepared_to_execute[&i_prepared_statement],
                    i_substitutions,
                    Some(paging_state)
                )
                .await?)
            } else if per_page_results > 0 {
                Arc::new(session.query_paged(
                    Query::new(&runtime_to_execute[&i_prepared_statement]).with_page_size(per_page_results.try_into().unwrap()),
                    i_substitutions,
                    Some(paging_state)
                )
                .await?)
            } else {
                Arc::new(session.query_paged(
                    Query::new(&runtime_to_execute[&i_prepared_statement]),
                    i_substitutions,
                    Some(paging_state)
                )
                .await?)
            }
        };

        let o_page = get_paging_state_from_result(&result_arc);
        let resout = get_json_rows_from_result(&result_arc)?;

        Ok(
            json!(
                {
                    "records": resout,
                    "records_number": resout.len(),
                    "paging_state": o_page
                }
            )
        )
    }

    pub async fn cql_delupsert(
        &self, 
        i_prepared_statement: String, 
        i_substitutions: impl ValueList,
        is_prepared: bool,
    ) -> Result<serde_json::Value, anyhow::Error> {

        let session = self.db_session.clone();
        let pepared_to_execute = self.p_queries.clone();
        let runtime_to_execute = self.p_runtime_queries.clone();

        if is_prepared {
            session.execute(
                &pepared_to_execute[&i_prepared_statement],
                i_substitutions,
            )
            .await?;
        } else {
            session.query(
                Query::new(&runtime_to_execute[&i_prepared_statement]),
                i_substitutions,
            )
            .await?;
        }

        Ok(
            json!(
                {
                    "records": [],
                    "records_number": 0,
                    "paging_state": ""
                }
            )
        )
    }
}

pub struct AppState {
    pub db_svc: ScyllaService,
    pub semaphore: Arc<Semaphore>,
    pub region: String
}