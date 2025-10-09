{{ config(materialized='table') }}

SELECT
	agent_id,
	agent_name,
	hire_date,
	branch,
	phone,
	email,
	status,
	load_timestamp as load_ts
FROM {{ source('raw','agent') }}
