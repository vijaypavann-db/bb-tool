-- depends_on: {{ ref('fact_event_replacement_impression') }}
-- depends_on: {{ ref('instadata_dwh_fact_event_replacement_customer_base') }}
-- depends_on: {{ ref('instadata_dwh_fact_event_replacement_shopper_base') }}
{% set incremental_lookback_predicate %}
WHERE
	replacement_converted_date_utc >= '{{ var('PREV_RUN_DATE_TIME') }}'::DATE
	AND replacement_converted_date_utc < '{{ var('RUN_DATE_TIME') }}'
{% endset %}
{{
	config(
		materialized='incremental',
		incremental_strategy='replace_where',
		on_schema_change = "append_new_columns",
		incremental_predicates = [incremental_lookback_predicate]
        
        
    )
}}
WITH
customer_replacements AS (
	SELECT
        -1 AS etl_run_id,
        'customer' AS app,
		f1.event_name AS event_name,
		f1.event_id AS replacement_conversion_event_id,
		f1.event_sent_date_time_utc AS replacement_conversion_sent_date_time_utc,
		f1.event_received_date_time_utc AS replacement_conversion_received_date_time_utc,
		f1.event_date_time_utc AS replacement_converted_date_time_utc,
		f1.event_date_utc AS replacement_converted_date_utc,
		f1.event_date_time_pt AS replacement_converted_date_time_pt,
		f1.event_date_pt AS replacement_converted_date_pt,
		f2.replacements_view_id AS replacements_view_id,
		f1.replacement_item_impression_id AS replacement_item_impression_id,
        'item_card' AS replacement_item_impression_type,
		f1.source_type AS source_type,
		coalesce(TRY_CAST(f1.source_value as AS DECIMAL(38, 0)), -1) AS original_item_id,
		f1.item_id AS converted_item_id,
		CASE WHEN f1.from_item_modal = 'Y' THEN 'item_modal' ELSE 'replacements_page' END AS conversion_source,
		f1.order_item_id AS order_item_id
	FROM {{ ref('eph_recent_fact_event_replacement_customer') }} f1
	LEFT JOIN {{ ref('eph_recent_fact_event_replacement_impression') }} f2
		ON f1.replacement_item_impression_id = f2.replacement_item_impression_id
		AND f2.app = 'customer'
		AND f2.event_name = 'store.item_card_impressions'
		AND (
			f1.replacements_view_id != f1.order_item_id OR datediff(
                'seconds', f1.event_date_time_utc, f2.replacement_impression_date_time_utc
            ) BETWEEN -120 AND 120
        )
	WHERE f1.event_name IN ('store.select_replacement', 'store.reject_replacement')
	AND lower(f1.source_type) NOT IN ('search', 'order_changes_replacements')
),
shopper_replacements AS (
	SELECT
    -1 AS etl_run_id,
    'shopper' AS app,
	f1.event_name AS event_name,
	f1.event_id AS replacement_conversion_event_id,
	f1.event_sent_date_time_utc AS replacement_conversion_sent_date_time_utc,
	f1.event_received_date_time_utc AS replacement_conversion_received_date_time_utc,
	f1.event_date_time_utc AS replacement_converted_date_time_utc,
	f1.event_date_utc AS replacement_converted_date_utc,
	f1.event_date_time_pt AS replacement_converted_date_time_pt,
	f1.event_date_pt AS replacement_converted_date_pt,
	f2.replacements_view_id AS replacements_view_id,
	f1.replacement_item_impression_id AS replacement_item_impression_id,
    'item_card' AS replacement_item_impression_type,
	f1.source_type AS source_type,
	coalesce(TRY_CAST(f1.source_value as AS DECIMAL(38, 0)), -1) AS original_item_id,
	f1.item_id AS converted_item_id,
	CASE WHEN f1.from_item_modal = 'Y' THEN 'item_modal' ELSE 'replacements_page' END AS conversion_source,
	f1.order_item_id AS order_item_id
	FROM {{ ref('eph_recent_fact_event_replacement_shopper') }} f1
	JOIN {{ ref('eph_recent_fact_event_replacement_impression') }} f2
		ON f1.replacement_item_impression_id = f2.replacement_item_impression_id
		AND f2.app = 'shopper'
		AND f2.event_name = 'replacements.item_card_impressions'
	WHERE f1.event_name = 'replacements.select_replacement'
	AND f1.source_type != 'search'
),
not_in_impression_shopper_replacements AS (
	SELECT
    -1 AS etl_run_id,
    'shopper' AS app,
	f1.event_name AS event_name,
	f1.event_id AS replacement_conversion_event_id,
	f1.event_sent_date_time_utc AS replacement_conversion_sent_date_time_utc,
	f1.event_received_date_time_utc AS replacement_conversion_received_date_time_utc,
	f1.event_date_time_utc AS replacement_converted_date_time_utc,
	f1.event_date_utc AS replacement_converted_date_utc,
	f1.event_date_time_pt AS replacement_converted_date_time_pt,
	f1.event_date_pt AS replacement_converted_date_pt,
	CAST(NULL AS STRING) AS replacements_view_id,
	f1.replacement_item_impression_id AS replacement_item_impression_id,
    'item_card' AS replacement_item_impression_type,
	f1.source_type AS source_type,
	coalesce(TRY_CAST(f1.source_value as AS DECIMAL(38, 0)), -1) AS original_item_id,
	f1.item_id AS converted_item_id,
	CASE WHEN f1.from_item_modal = 'Y' THEN 'item_modal' ELSE 'replacements_page' END AS conversion_source,
	f1.order_item_id AS order_item_id
	FROM {{ ref('eph_recent_fact_event_replacement_shopper') }} f1
	WHERE f1.event_name = 'replacements.select_replacement'
	AND nvl(f1.replacement_item_impression_id, '-100') NOT IN (
			SELECT replacement_item_impression_id
			FROM {{ ref('eph_recent_fact_event_replacement_impression') }}
			WHERE app = 'shopper'
			AND replacement_item_impression_id IS NOT NULL
    )
),
fact_event_replacement_conversion_stage AS (
	SELECT * FROM customer_replacements
	UNION ALL
	SELECT * FROM shopper_replacements
	UNION ALL
	SELECT * FROM not_in_impression_shopper_replacements
)
SELECT
	etl_run_id,
	app,
	event_name,
	replacement_conversion_event_id,
	replacement_conversion_sent_date_time_utc,
	replacement_conversion_received_date_time_utc,
	replacement_converted_date_time_utc,
	replacement_converted_date_utc,
	replacement_converted_date_time_pt,
	replacement_converted_date_pt,
	replacements_view_id,
	replacement_item_impression_id,
	replacement_item_impression_type,
	source_type,
	original_item_id,
	converted_item_id,
	conversion_source,
	order_item_id,
	cast(current_timestamp() AS TIMESTAMP) AS dwh_created_date_time_utc
FROM fact_event_replacement_conversion_stage
{% if is_incremental() %}
	 {{incremental_lookback_predicate}}
{% endif %}