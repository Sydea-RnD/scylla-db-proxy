use serde_json::json;

pub fn get_statements() -> serde_json::Value {
    json!(
        {
            // Example statement
            // "YOUR_STATEMENT_NAME": {
            //     "statement": r#"
            //         SELECT JSON *
            //         FROM your_table
            //         WHERE your_field = ? ;
            //     "#,
            //     "is_query": true,
            //     "is_paged": false,
            //     "per_page_results": 0,
            //     "is_prepared": true,
            //     "casting": {
            //         "0": "Text"
            //     }
            // }
        }
    )
}