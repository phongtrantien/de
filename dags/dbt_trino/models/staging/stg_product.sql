{{ config(materialized='table') }}

SELECT
	product_id,
	product_name,
	product_type,
	null as coverage_amount,
	null as premium_rate,
	created_at as effective_date,
	created_at as expiry_date,
	updated_at as load_ts
FROM {{ source('raw','product') }}
