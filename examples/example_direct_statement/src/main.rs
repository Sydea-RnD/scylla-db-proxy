use serde::Deserialize;
use serde_derive::{Deserialize, Serialize};
use hyper_tls::HttpsConnector;
use hyper;
use serde_json::json;

const SCYLLA_ENDPOINT: &str = "https://scylla.example.com/v2/direct_statement";
const PER_PAGE_RESULT: u64 = 10;

#[derive(Debug, Clone, Deserialize)]
struct MyTable {
    foo: Option<String>, 
    bar: Option<String>
}

async fn cql<T: for<'a> Deserialize<'a>>(client: &hyper::Client<HttpsConnector<hyper::client::connect::HttpConnector>>, cql_statement: String, paging: String) -> Result<(Vec<T>, String), anyhow::Error> {
    
    println!("{}", cql_statement);

    let req = match hyper::Request::builder()
        .method(hyper::Method::from_bytes(String::from("POST").as_bytes()).unwrap())
        .uri(SCYLLA_ENDPOINT)
        .body(hyper::Body::from(json!({
            "operation": [
                {
                    "statement_id": "GENERIC_STATEMENT",
                    "statement": cql_statement,
                    "paging": paging,
                    "per_page_results": PER_PAGE_RESULT
                }
            ]
        }).to_string())) {
            Ok(v) => v,
            Err(e) => return Err(e.into())
    };

    let resp = match client.request(req).await {
        Ok(v) => v,
        Err(e) => return Err(e.into())
    };

    let body_bytes = match hyper::body::to_bytes(resp.into_body()).await {
        Ok(v) => v,
        Err(e) => return Err(e.into())
    };
    let body_string = match String::from_utf8(body_bytes.to_vec()) {
        Ok(w) => w,
        Err(e) => return Err(e.into())
    };
    let body_json: serde_json::Value = match serde_json::from_str(&body_string) {
        Ok(u) => u,
        Err(e) => return Err(e.into())
    };
    
    let result_data = body_json.as_object().unwrap()["GENERIC_STATEMENT"].as_object().unwrap();
    let paging_state = result_data.clone()["paging_state"].as_str().unwrap().to_string();
    let records: Vec<T> = serde_json::from_str(&result_data.clone()["records"].to_string()).unwrap();

    Ok((records, paging_state))
}

fn init_hyper_client() -> hyper::Client<HttpsConnector<hyper::client::connect::HttpConnector>> {

    let https = HttpsConnector::new();
    let client = hyper::Client::builder().build::<_, hyper::Body>(https);
    client
}

#[tokio::main]
async fn main() {

    let my_baz: String = String::from("test");

    let hyper_client = init_hyper_client();

    let my_statement_cql = format!(r#"
        SELECT JSON foo, bar
        FROM my_schema.my_table
        WHERE baz = '{my_baz}'
          AND is_premium = true
        ORDER BY rank asc, total_time asc
        ALLOW FILTERING;
    "#);
    let (my_table, my_paging_state) = match cql::<MyTable>(&hyper_client, my_statement_cql, "".to_string()).await {
        Ok(v) => v,
        Err(e) => panic!("{}", e.to_string())
    };

    println!("TABLE DATA {:#?}", my_table);
    println!("PAGING STATE {:#?}", my_paging_state);
}
