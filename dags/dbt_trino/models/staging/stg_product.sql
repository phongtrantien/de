{{ config(materialized='table') }}

SELECT
	product_id,
	product_name,
	product_type,
	coverage_amount,
	premium_rate,
	effective_date,
	expiry_date,
	load_timestamp as load_ts
FROM {{ source('raw','product') }}
