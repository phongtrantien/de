{{ config(materialized='table') }}

SELECT
  claim_id,
  policy_id,
  claim_date,
  claim_amount,
  claim_status,
  updated_at as load_ts
FROM {{ source('raw','claim') }}

