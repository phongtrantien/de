{{ config(materialized='table') }}

WITH latest_customer AS (
  SELECT
    hk_customer,
    full_name,
    dob,
    address,
    phone,
    email,
    load_ts,
    ROW_NUMBER() OVER (PARTITION BY hk_customer ORDER BY load_ts DESC) rn
  FROM {{ ref('sat_customer') }}
)
SELECT
  c.hk_customer,
  c.full_name,
  c.dob,
  c.address,
  c.phone,
  c.email,
  c.load_ts
FROM latest_customer c
WHERE c.rn = 1

