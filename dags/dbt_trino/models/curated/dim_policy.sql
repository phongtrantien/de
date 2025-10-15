{{ config(materialized='table') }}

WITH latest_policy AS (
  SELECT
    hk_policy,
    policy_id,
#    product_name,
    start_date,
    end_date,
    load_ts,
    ROW_NUMBER() OVER (PARTITION BY hk_policy ORDER BY load_ts DESC) rn
  FROM {{ ref('sat_policy') }}
)
SELECT
  p.hk_policy,
  p.policy_number,
  p.product_code,
  p.start_date,
  p.end_date,
  p.load_ts
FROM latest_policy p
WHERE p.rn = 1

