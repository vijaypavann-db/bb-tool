{{
	config(
		materialized='incremental',
		incremental_strategy='delete+insert',
		unique_key='replacement_impression_date_utc',
		snowflake_warehouse=get_snowflake_warehouse('DE_DBT_2XLARGE_WH')
    );};

SELECT
    -1 AS etl_run_id,
    'customer' AS app,
	event_name AS event_name,
	event_id AS replacement_impression_event_id,
	replacements_view_id AS replacements_view_id,
	event_sent_date_time_utc AS replacement_impression_sent_date_time_utc,
	event_received_date_time_utc AS replacement_impression_received_date_time_utc,
	event_date_time_utc AS replacement_impression_date_time_utc,
	event_date_utc AS replacement_impression_date_utc,
	event_date_time_pt AS replacement_impression_date_time_pt,
	event_date_pt AS replacement_impression_date_pt,
	source_type AS source_type,
	item_card_impression_id AS replacement_item_impression_id,
    'item_card' AS replacement_item_impression_type,
	item_card_impressions_item_id AS item_id,
	item_card_impression_model_score AS model_score,
	item_card_impression_display_position AS display_position,
	item_card_impression_impression_attributes AS item_impression_attributes,
	item_card_impression_grid_column AS grid_column,
	item_card_impression_grid_row AS grid_row,
	NULL::STRING AS first_view_item_event_id,
    'N' AS viewed_item_ind,
	NULL::DATETIME AS first_view_item_date_time_utc,
	propensity AS propensity,
	CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9)) AS dwh_created_date_time_utc
FROM {{ ref('instadata_dwh_fact_event_replacement_customer_base');};
WHERE event_name = 'store.item_card_impressions'
{% if is_incremental() %};
	AND replacement_impression_date_utc >= '{var('PREV_RUN_DATE_TIME');}'::DATE
	AND replacement_impression_date_utc < '{var('RUN_DATE_TIME');}'
{% endif %};