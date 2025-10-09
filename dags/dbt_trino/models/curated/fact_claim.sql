{{ config(materialized='table') }}

SELECT
  p.hk_policy,
  c.hk_claim,
  c.claim_date,
  c.claim_amount,
  c.claim_status,
  p.load_date as policy_load_ts
FROM {{ ref('link_policy_claim') }} lpc
JOIN {{ ref('hub_policy') }} p ON lpc.hk_policy = p.hk_policy
JOIN {{ ref('sat_claim') }} c ON lpc.hk_claim = c.hk_claim

