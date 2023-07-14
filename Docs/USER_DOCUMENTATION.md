# User Documentation

### This is the documentation for the final user of the scylla-db-proxy project.

## Project setup

**TBD**

## Project endpoints and payloads

The scylla-db-proxy project exposes 2 endpoints:
1. /v2/execute_statement
2. /v2/direct_statement

#### /v2/execute_statement
The first one allows you to execute a statement that is already saved in the **queries.rs** file of the project.
So all you will need to do to execute a statement with this endpoint is sending this payload:

```json

{
	"operation": [
		{
			"statement_id": "YOUR_STATEMENT_NAME",
			"query_data": [
				"first_value",
				"second_value"
			], 
			"paging": ""
		}
	]
}

```

In "query_data" the values are *positional*, so the first one is going in the first parameter, the second one in the second parameter and so on...

#### /v2/direct_statement
The second endpoint allows you to send directly a string query to scylla-db-proxy. 

The payload you have to send to the service is:

```json

{
	"operation": [
		{
			"statement_id": "YOUR_STATEMENT_NAME",
			"statement": "SELECT JSON * FROM my_schema.my_stable WHERE my_field = 'my_value';",
			"paging": "",
			"per_page_results": 0
		}
	]
}

```

An example for this endpoint is available in examples/example_direct_statement.