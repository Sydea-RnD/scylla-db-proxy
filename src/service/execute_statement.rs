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
use scylla::frame::value::Value;
use std::str::FromStr;
use scylla::frame::response::result::CqlValue;

async fn execute_single_statement(i_statement: String, i_paging: String, i_query_data: &Vec<serde_json::Value>, state: &AppState) -> Result<serde_json::Value, serde_json::Value> {

    let mut query_data: Vec<Box<dyn Value>> = vec![];

    for (idx, lline) in i_query_data.iter().enumerate() {
        let scylla_value_type: String = state.db_svc.p_queries_attributes[&i_statement]["casting"][&idx.to_string()].as_str().unwrap().to_string();
        let logging_str = format!("statement: {} - value: {} - casting: {}", i_statement, lline.to_string(), scylla_value_type);
        let boxed_value_to_push: Box<dyn Value> = match cast_json_value_to_scylla_value(
            lline, 
            scylla_value_type,
            logging_str
        ) {
            Ok(v) => Box::new(v),
            Err(e) => return Err(e)
        };
        query_data.push(boxed_value_to_push);
    };

    let rj = match state.db_svc.cql_statement(
        i_statement,
        query_data,
        i_paging
    ).await {
        Ok(v) => v,
        Err(e) => return Err(
            erfn(500, e.to_string(), "scylla_error".to_string(), e.to_string())
        )
    };

    Ok(rj)

}

fn cast_json_value_to_scylla_value(i_json_value: &serde_json::Value, i_scylla_value_type: String, i_logging_str: String) -> Result<CqlValue, serde_json::Value> {

    let r_cql_value_result = match &*i_scylla_value_type {
        "Boolean" => convert_json_value_to_boolean(i_json_value, &i_logging_str),
        "Decimal" => convert_json_value_to_decimal(i_json_value, &i_logging_str),
        "Double" => convert_json_value_to_double(i_json_value, &i_logging_str),
        "Empty" => convert_json_value_to_empty(i_json_value, &i_logging_str),
        "Float" => convert_json_value_to_float(i_json_value, &i_logging_str),
        "Int" => convert_json_value_to_int(i_json_value, &i_logging_str),
        "BigInt" => convert_json_value_to_bigint(i_json_value, &i_logging_str),
        "Text" => convert_json_value_to_text_as_str(i_json_value),
        "Map" => convert_json_value_to_text(i_json_value),
        "SmallInt" => convert_json_value_to_smallint(i_json_value, &i_logging_str),
        "TinyInt" => convert_json_value_to_tinyint(i_json_value, &i_logging_str),
        &_ => return Err(
            erfn(
                500, 
                i_logging_str.to_string(),
                "conversion_for_type_not_implemented".to_string(), 
                format!("conversion_for_type_{}_not_implemented", i_scylla_value_type)
            )
        ),
        // //"Ascii" => convert_json_value_to_ascii(i_json_value),              ////Ascii(String)
        // //"Blob" => convert_json_value_to_blob(i_json_value),                ////Blob(Vec<u8, Global>)
        // //"Counter" => convert_json_value_to_counter(i_json_value),          ////Counter(Counter)            
        // //"Date" => convert_json_value_to_date(i_json_value),                ////Date(u32)
        // //"Duration" => convert_json_value_to_duration(i_json_value),        ////Duration(CqlDuration)
        // //"Timestamp" => convert_json_value_to_timestamp(i_json_value),      ////Timestamp(Duration)
        // //"Inet" => convert_json_value_to_inet(i_json_value),                ////Inet(IpAddr)
        // //"List" => convert_json_value_to_list(i_json_value),                ////List(Vec<CqlValue, Global>)
        // //"Map" => convert_json_value_to_map(i_json_value),                  ////Map(Vec<(CqlValue, CqlValue), Global>)
        // //"Set" => convert_json_value_to_set(i_json_value),                  ////Set(Vec<CqlValue, Global>)
        // //"UserDefinedType" =>  convert_json_value_to_udt(i_json_value),     ////UserDefinedType { keyspace: String, type_name: String, fields: Vec<(String, Option<CqlValue>), Global> }
        // //"Time" => convert_json_value_to_time(i_json_value),                ////Time(Duration)
        // //"Timeuuid" => convert_json_value_to_timeuuid(i_json_value),        ////Timeuuid(Uuid)
        // //"Tuple" => convert_json_value_to_tuple(i_json_value),              ////Tuple(Vec<Option<CqlValue>, Global>)
        // //"Uuid" => convert_json_value_to_uuid(i_json_value),                ////Uuid(Uuid)
        // //"Varint" => convert_json_value_to_varint(i_json_value)             ////Varint(BigInt)
    };

    let r_cql_value = match r_cql_value_result {
        Ok(v) => {
            println!("{}", &i_logging_str);
            v
        },
        Err(e) => return Err(e)
    };

    Ok(r_cql_value)
}

fn convert_json_value_to_boolean(i_json_value: &serde_json::Value, i_logging_str: &String) -> Result<CqlValue, serde_json::Value> {
    if !i_json_value.is_boolean() {
        return Err(
            erfn(500, i_logging_str.to_string(), "value_is_not_bool".to_string(), "value_is_not_bool".to_string())
        )
    }
    let r_value = match i_json_value.as_bool() {
        Some(x) => x,
        None => return Err(
            erfn(500, i_logging_str.to_string(), "boolean_is_none".to_string(), "boolean_is_none".to_string())
        )
    };
    Ok(scylla::frame::response::result::CqlValue::Boolean(r_value))
}

fn convert_json_value_to_double(i_json_value: &serde_json::Value, i_logging_str: &String) -> Result<CqlValue, serde_json::Value> {
    if !i_json_value.is_f64() {
        return Err(
            erfn(500, i_logging_str.to_string(), "value_is_not_f64".to_string(), "value_is_not_f64".to_string())
        )
    }
    let r_value = match i_json_value.as_f64() {
        Some(x) => x,
        None => return Err(
            erfn(500, i_logging_str.to_string(), "double_is_none".to_string(), "double_is_none".to_string())
        )
    };
    Ok(scylla::frame::response::result::CqlValue::Double(r_value))
}

fn convert_json_value_to_float(i_json_value: &serde_json::Value, i_logging_str: &String) -> Result<CqlValue, serde_json::Value> {
    if !i_json_value.is_f64() {
        return Err(
            erfn(500, i_logging_str.to_string(), "value_is_not_f64".to_string(), "value_is_not_f64".to_string())
        )
    }
    let r_value = match i_json_value.as_f64() {
        Some(x) => x as f32,
        None => return Err(
            erfn(500, i_logging_str.to_string(), "float_is_none".to_string(), "float_is_none".to_string())
        )
    };
    Ok(scylla::frame::response::result::CqlValue::Float(r_value))
}

fn convert_json_value_to_int(i_json_value: &serde_json::Value, i_logging_str: &String) -> Result<CqlValue, serde_json::Value> {
    if !i_json_value.is_i64() {
        return Err(
            erfn(500, i_logging_str.to_string(), "value_is_not_i64".to_string(), "value_is_not_i64".to_string())
        )
    }
    let integer_value = match i_json_value.as_i64() {
        Some(x) => x,
        None => return Err(
            erfn(500, i_logging_str.to_string(), "int_is_none".to_string(), "int_is_none".to_string())
        )
    };
    let r_value: i32 = match integer_value.try_into() {
        Ok(v) => v,
        Err(e) => return Err(
            erfn(500, i_logging_str.to_string(), "conversion_error_i64_i32".to_string(), e.to_string())
        )
    };
    Ok(scylla::frame::response::result::CqlValue::Int(r_value))
}

fn convert_json_value_to_smallint(i_json_value: &serde_json::Value, i_logging_str: &String) -> Result<CqlValue, serde_json::Value> {
    if !i_json_value.is_i64() {
        return Err(
            erfn(500, i_logging_str.to_string(), "value_is_not_i64".to_string(), "value_is_not_i64".to_string())
        )
    }
    let integer_value = match i_json_value.as_i64() {
        Some(x) => x,
        None => return Err(
            erfn(500, i_logging_str.to_string(), "int_is_none".to_string(), "int_is_none".to_string())
        )
    };
    let r_value: i16 = match integer_value.try_into() {
        Ok(v) => v,
        Err(e) => return Err(
            erfn(500, i_logging_str.to_string(), "conversion_error_i64_i16".to_string(), e.to_string())
        )
    };
    Ok(scylla::frame::response::result::CqlValue::SmallInt(r_value))
}

fn convert_json_value_to_tinyint(i_json_value: &serde_json::Value, i_logging_str: &String) -> Result<CqlValue, serde_json::Value> {
    if !i_json_value.is_i64() {
        return Err(
            erfn(500, i_logging_str.to_string(), "value_is_not_i64".to_string(), "value_is_not_i64".to_string())
        )
    }
    let integer_value = match i_json_value.as_i64() {
        Some(x) => x,
        None => return Err(
            erfn(500, i_logging_str.to_string(), "int_is_none".to_string(), "int_is_none".to_string())
        )
    };
    let r_value: i8 = match integer_value.try_into() {
        Ok(v) => v,
        Err(e) => return Err(
            erfn(500, i_logging_str.to_string(), "conversion_error_i64_i8".to_string(), e.to_string())
        )
    };
    Ok(scylla::frame::response::result::CqlValue::TinyInt(r_value))
}

fn convert_json_value_to_text(i_json_value: &serde_json::Value) -> Result<CqlValue, serde_json::Value> {
    let r_value: String = i_json_value.to_string();
    Ok(scylla::frame::response::result::CqlValue::Text(r_value))
}

fn convert_json_value_to_text_as_str(i_json_value: &serde_json::Value) -> Result<CqlValue, serde_json::Value> {
    let r_value: String = i_json_value.as_str().unwrap().to_string();
    Ok(scylla::frame::response::result::CqlValue::Text(r_value))
}

fn convert_json_value_to_bigint(i_json_value: &serde_json::Value, i_logging_str: &String) -> Result<CqlValue, serde_json::Value> {
    if !i_json_value.is_i64() {
        return Err(
            erfn(500, i_logging_str.to_string(), "value_is_not_i64".to_string(), "value_is_not_i64".to_string())
        )
    }
    let r_value = match i_json_value.as_i64() {
        Some(x) => x,
        None => return Err(
            erfn(500, i_logging_str.to_string(), "int_is_none".to_string(), "int_is_none".to_string())
        )
    };
    Ok(scylla::frame::response::result::CqlValue::BigInt(r_value))
}

fn convert_json_value_to_decimal(i_json_value: &serde_json::Value, i_logging_str: &String) -> Result<CqlValue, serde_json::Value> {
    let r_value: bigdecimal::BigDecimal = match bigdecimal::BigDecimal::from_str(&i_json_value.to_string()){
        Ok(n) => n,
        Err(e) => return Err(
            erfn(500, i_logging_str.to_string(), e.to_string(), e.to_string())
        )
    };
    Ok(scylla::frame::response::result::CqlValue::Decimal(r_value))
}

fn convert_json_value_to_empty(i_json_value: &serde_json::Value, i_logging_str: &String) -> Result<CqlValue, serde_json::Value> {
    if !i_json_value.is_null() {
        return Err(
            erfn(500, i_logging_str.to_string(), "value_is_not_null".to_string(), "value_is_not_null".to_string())
        )
    }
    match i_json_value.as_null() {
        Some(x) => x,
        None => return Err(
            erfn(500, i_logging_str.to_string(), "null_is_none".to_string(), "null_is_none".to_string())
        )
    };
    Ok(scylla::frame::response::result::CqlValue::Empty)
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
            lline_d["statement_id"].as_str().unwrap().to_string(),
            lline_d["paging"].as_str().unwrap().to_string(),
            lline_d["query_data"].as_array().unwrap(),
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
    if !oj.as_object().unwrap().contains_key("query_data") {
        return Err(
            erfn(400, "no_query_data_in_request".to_string(), "no_query_data_in_request".to_string(), "no_query_data_in_request".to_string())
        )
    };
    if !oj.as_object().unwrap().contains_key("statement_id") {
        return Err(
            erfn(400, "no_statement_id_in_request".to_string(), "no_statement_id_in_request".to_string(), "no_statement_id_in_request".to_string())
        )
    };
    if !oj.as_object().unwrap().contains_key("paging") {
        return Err(
            erfn(400, "no_paging_in_request".to_string(), "no_paging_in_request".to_string(), "no_paging_in_request".to_string())
        )
    };
    if !oj["query_data"].is_array() {
        return Err(
            erfn(400, "query_data_must_be_an_array".to_string(), "query_data_must_be_an_array".to_string(), "query_data_must_be_an_array".to_string())
        )
    };
    if !oj["statement_id"].is_string() {
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