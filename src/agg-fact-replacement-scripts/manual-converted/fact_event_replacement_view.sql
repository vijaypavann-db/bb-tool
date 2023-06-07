{% set lookback_predicate %}
    replacement_view_event_date_utc >= {{ get_prev_run_date() }} AND replacement_view_event_date_utc < {{ get_run_date() }}
{% endset %}

{% set incremental_lookback_predicate %}
    replacement_view_event_date_utc >= {{ get_prev_run_date() }} AND replacement_view_event_date_utc < {{ get_run_date() }}
{% endset %}

{{
    config(
        materialized='incremental',
        alias= "fact_event_replacement_view",
        on_schema_change = "append_new_columns",
        incremental_strategy="replace_where",
        incremental_predicates=[incremental_lookback_predicate]
        )
}}


WITH

customer_rc AS (
SELECT
  replacement_conversion_event_id,
  event_name,
  replacement_converted_date_time_utc,
  replacements_view_id,
  ROW_NUMBER() OVER (
      PARTITION BY
          replacements_view_id
      ORDER BY replacement_converted_date_time_utc DESC NULLS FIRST
  ) AS rank
FROM {{ ref('eph_recent_fact_event_replacement_conversion') }}
WHERE
  app = 'customer'
  AND event_name IN ('store.select_replacement', 'store.reject_replacement')
),

customer_replacements AS (
 SELECT
  -1 AS etl_run_id,
  'customer' AS app,
  CASE
    WHEN LOWER(f1.event_name) = 'store.replacements_view'
    THEN 'store.view_replacements'
    ELSE LOWER(f1.event_name)
  END AS event_name,
  f1.event_id AS replacement_view_event_id,
  f1.event_sent_date_time_utc AS replacement_view_event_sent_date_time_utc,
  f1.event_received_date_time_utc AS replacement_view_event_received_date_time_utc,
  f1.event_date_time_utc AS replacement_view_event_date_time_utc,
  f1.event_date_utc AS replacement_view_event_date_utc,
  f1.event_date_time_pt AS replacement_view_event_date_time_pt,
  f1.event_date_pt AS replacement_view_event_date_pt,
  f1.replacements_view_id AS replacements_view_id,
  f1.parent_id AS parent_id,
  f1.num_replacements AS num_replacements,
  f1.total_replacements AS total_replacements,
  f1.platform AS platform,
  CASE
    WHEN LOWER(f1.source_type) = 'high_confidence_replacement'
    THEN 'high_confidence_replacements'
    WHEN LOWER(f1.source_type) = 'user_choice_replacements'
    THEN 'user_chosen_replacements'
    ELSE LOWER(f1.source_type)
  END AS source_type,
  COALESCE(TRY_CAST(f1.source_value AS DECIMAL(38, 0)), -1) AS source_value,
  CAST(NULL AS STRING) AS search_term,
  'N' AS exclude_ind,
  IFNULL(fetch_error_ind, 'N') AS fetch_error_ind,
  COALESCE(TRY_CAST(user_id AS DECIMAL(38, 0)), -1) AS user_id,
  CAST(NULL AS DECIMAL(38, 0)) AS shopper_id,
  CAST(NULL AS STRING) AS shopper_roles,
  CAST(NULL AS DECIMAL(38, 0)) AS shopper_shift_id,
  CAST(NULL AS STRING) AS shift_type,
  CAST(NULL AS DECIMAL(38, 0)) AS batch_id,
  order_item_id AS order_item_id,
  order_delivery_id AS order_delivery_id,
  order_id AS order_id,
  CAST(api_version AS INT) AS api_version,
  app_version AS app_version,
  warehouse_id AS warehouse_id,
  inventory_area_id AS inventory_area_id,
  zone_id AS zone_id,
  zip_code AS zip_code,
  whitelabel_retailer AS whitelabel_retailer,
  CASE
      WHEN IFNULL(whitelabel_id, 1) <> 1 THEN 'Y' ELSE 'N'
  END AS whitelabel_ind,
  wl_exclusive AS wl_exclusive,
  country_id AS country_id,
  CASE
    WHEN rc.rank = 1 AND rc.event_name IN ('store.select_replacement')
    THEN 'Y'
    ELSE 'N'
  END AS converted_ind,
  rc.replacement_conversion_event_id AS last_replacement_conversion_event_id,
  rc.replacement_converted_date_time_utc AS last_replacement_conversion_date_time_utc,
  f1.candidates AS candidates
FROM {{ ref('eph_recent_fact_event_replacement_customer') }} AS f1
LEFT JOIN customer_rc AS rc
  ON f1.replacements_view_id = rc.replacements_view_id
  AND rc.rank = 1
  AND (
    f1.replacements_view_id <> f1.order_item_id
    OR (
        UNIX_TIMESTAMP(
            f1.event_date_time_utc
        ) - UNIX_TIMESTAMP(
            rc.replacement_converted_date_time_utc
        ) BETWEEN -120 AND 120
    )
  )
WHERE
  f1.event_name IN ('store.replacements_view', 'store.view_replacements')
  AND f1.source_type <> 'search'
),

shopper_rc AS (
    SELECT
        replacement_conversion_event_id,
        replacement_converted_date_time_utc,
        replacements_view_id,
        ROW_NUMBER() OVER (
            PARTITION BY replacements_view_id
            ORDER BY replacement_converted_date_time_utc DESC
        ) AS rank
    FROM {{ ref('eph_recent_fact_event_replacement_conversion') }}
    WHERE app = 'shopper'
    AND event_name = 'replacements.select_replacement'
),

shopper_replacements AS (
  SELECT
  -1 AS etl_run_id,
  'shopper' AS app,
  CASE
    WHEN LOWER(f1.event_name) = 'replacements.replacements_view'
    THEN 'replacements.view_replacements'
    ELSE LOWER(f1.event_name)
  END AS event_name,
  f1.event_id AS replacement_view_event_id,
  f1.event_sent_date_time_utc AS replacement_view_event_sent_date_time_utc,
  f1.event_received_date_time_utc AS replacement_view_event_received_date_time_utc,
  f1.event_date_time_utc AS replacement_view_event_date_time_utc,
  f1.event_date_utc AS replacement_view_event_date_utc,
  f1.event_date_time_pt AS replacement_view_event_date_time_pt,
  f1.event_date_pt AS replacement_view_event_date_pt,
  f1.replacements_view_id AS replacements_view_id,
  f1.parent_id AS parent_id,
  f1.num_replacements AS num_replacements,
  f1.total_replacements AS total_replacements,
  f1.platform AS platform,
  CASE
    WHEN LOWER(f1.source_type) = 'high_confidence_replacement'
    THEN 'high_confidence_replacements'
    WHEN LOWER(f1.source_type) = 'user_choice_replacements'
    THEN 'user_chosen_replacements'
    ELSE LOWER(f1.source_type)
  END AS source_type,
  COALESCE(TRY_CAST(f1.source_value AS DECIMAL(38, 0)), -1) AS source_value,
  f1.search_term AS search_term,
  'N' AS exclude_ind,
  IFNULL(fetch_error_ind, 'N') AS fetch_error_ind,
  COALESCE(TRY_CAST(user_id AS DECIMAL(38, 0)), -1) AS user_id,
  shopper_id AS shopper_id,
  shopper_roles AS shopper_roles,
  shopper_shift_id AS shopper_shift_id,
  shift_type AS shift_type,
  batch_id AS batch_id,
  order_item_id AS order_item_id,
  order_delivery_id AS order_delivery_id,
  order_id AS order_id,
  CAST(NULL AS DECIMAL) AS api_version,
  app_version AS app_version,
  warehouse_id AS warehouse_id,
  inventory_area_id AS inventory_area_id,
  zone_id AS zone_id,
  zip_code AS zip_code,
  CAST(NULL AS STRING) AS whitelabel_retailer,
  CAST(NULL AS STRING) AS whitelabel_ind,
  CAST(NULL AS STRING) AS wl_exclusive,
  country_id AS country_id,
  CASE WHEN rc.rank = 1 THEN 'Y' ELSE 'N' END AS converted_ind,
  rc.replacement_conversion_event_id AS last_replacement_conversion_event_id,
  rc.replacement_converted_date_time_utc AS last_replacement_conversion_date_time_utc,
  CAST(NULL AS STRING) AS candidates
FROM {{ ref('eph_recent_fact_event_replacement_shopper') }} AS f1
LEFT JOIN shopper_rc AS rc
  ON f1.replacements_view_id = rc.replacements_view_id AND rc.rank = 1
WHERE
  f1.event_name IN (
      'replacements.replacements_view', 'replacements.view_replacements'
  )
),

fact_event_replacement_stage AS (
    SELECT * FROM customer_replacements
    UNION ALL
    SELECT * FROM shopper_replacements
)

SELECT
    fact_event_replacement_stage.*,
    CAST(NULL AS DECIMAL(38, 0)) AS device_id,
    CAST(CURRENT_TIMESTAMP() AS TIMESTAMP) AS dwh_created_date_time_utc
FROM fact_event_replacement_stage
{% if is_incremental() %}
WHERE
    {{ lookback_predicate }}
{% endif %}
