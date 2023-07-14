use actix_web::{
    web, 
    Error, 
    HttpRequest, 
    HttpResponse,
    http::{header::ContentType, StatusCode}
};
use serde_json::json;
use crate::common::{decode_status_code, init_input, AppState, erfn};
use anyhow::Result;

async fn execute_single_statement(i_statement: String, i_paging: String, i_per_page_results: u64, state: &AppState) -> Result<serde_json::Value, serde_json::Value> {

    let rj = match state.db_svc.direct_statement(
        i_statement,
        i_per_page_results,
        i_paging
    ).await {
        Ok(v) => v,
        Err(e) => return Err(
            erfn(500, e.to_string(), "scylla_error".to_string(), e.to_string())
        )
    };

    Ok(rj)

}

async fn main_logic(request: HttpRequest, body: web::Bytes, state: &AppState) -> Result<serde_json::Value, serde_json::Value> {

    let oj: serde_json::Value = match init_input(request, body) {
        Ok(v) => match check_input_data(v) {
            Ok(w) => w,
            Err(f) => return Err(f)
        },
        Err(e) => return Err(e)
    };

    let mut rj = json!({});

    for lline in oj["body"]["operation"].as_array().unwrap() {
        let lline_d = match check_input_data_single_line(lline) {
            Ok(v) => v,
            Err(e) => return Err(e)
        };
        rj[lline_d["statement_id"].as_str().unwrap().to_string()] = match execute_single_statement(
            lline_d["statement"].as_str().unwrap().to_string(),
            lline_d["paging"].as_str().unwrap().to_string(),
            lline_d["per_page_results"].as_u64().unwrap(),
            state
        ).await {
            Ok(v) => v,
            Err(e) => return Err(e)
        };
    }

    Ok(rj)
}

pub fn check_input_data(oj: serde_json::Value) -> Result<serde_json::Value, serde_json::Value> {
    if !oj["body"].as_object().unwrap().contains_key("operation") {
        return Err(
            erfn(400, "no_operation_in_request".to_string(), "no_operation_in_request".to_string(), "no_operation_in_request".to_string())
        )
    };
    if !oj["body"]["operation"].is_array() {
        return Err(
            erfn(400, "operation_must_be_an_array".to_string(), "operation_must_be_an_array".to_string(), "operation_must_be_an_array".to_string())
        )
    };
    Ok(oj)
}

pub fn check_input_data_single_line(oj: &serde_json::Value) -> Result<&serde_json::Value, serde_json::Value> {
    if !oj.as_object().unwrap().contains_key("statement") {
        return Err(
            erfn(400, "no_query_data_in_request".to_string(), "no_query_data_in_request".to_string(), "no_query_data_in_request".to_string())
        )
    };
    if !oj.as_object().unwrap().contains_key("per_page_results") {
        return Err(
            erfn(400, "no_statement_id_in_request".to_string(), "no_statement_id_in_request".to_string(), "no_statement_id_in_request".to_string())
        )
    };
    if !oj.as_object().unwrap().contains_key("paging") {
        return Err(
            erfn(400, "no_paging_in_request".to_string(), "no_paging_in_request".to_string(), "no_paging_in_request".to_string())
        )
    };
    if !oj.as_object().unwrap().contains_key("statement_id") {
        return Err(
            erfn(400, "no_query_data_in_request".to_string(), "no_query_data_in_request".to_string(), "no_query_data_in_request".to_string())
        )
    };
    if !oj["statement"].is_string() {
        return Err(
            erfn(400, "query_data_must_be_an_array".to_string(), "query_data_must_be_an_array".to_string(), "query_data_must_be_an_array".to_string())
        )
    };
    if !oj["statement_id"].is_string() {
        return Err(
            erfn(400, "query_data_must_be_an_array".to_string(), "query_data_must_be_an_array".to_string(), "query_data_must_be_an_array".to_string())
        )
    };
    if !oj["per_page_results"].is_number() {
        return Err(
            erfn(400, "statement_id_must_be_a_string".to_string(), "statement_id_must_be_a_string".to_string(), "statement_id_must_be_a_string".to_string())
        )
    };
    if !oj["paging"].is_string() {
        return Err(
            erfn(400, "paging_must_be_a_string".to_string(), "paging_must_be_a_string".to_string(), "paging_must_be_a_string".to_string())
        )
    };
    Ok(oj)
}

pub async fn index(request: HttpRequest, body: web::Bytes, state: web::Data<AppState>) -> Result<HttpResponse, Error> {
    let state: &AppState = &state;
    let oj: serde_json::Value = match main_logic(request, body, state).await {
        Ok(v) => v,
        Err(e) => {
            let ret_status_code: u16 = match decode_status_code(&e["status_code"]) {
                Ok(n) => n,
                Err(_e) => 500
            };
            println!("ERRORE - RESPONSE {}", e.to_string());
            return Ok(HttpResponse::build(StatusCode::from_u16(ret_status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
            .insert_header(ContentType::json())
            .body(e.to_string()))
        }
    };
    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(oj.to_string()))
}