{{ config(materialized='table') }}

SELECT
	agent_id,
	agent_name,
	created_at as hire_date,
	region as branch,
	phone,
	email,
	updated_at as load_ts
FROM {{ source('raw','agent') }}
