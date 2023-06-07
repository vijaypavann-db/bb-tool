/* P.S:
    1. All the data returned by lookback_predicate has to satisfy the condition in incremental_lookback_predicate (i.e. the replace where clause), otherwise the insert will fail.
        with Error:  "Data written out does not match replaceWhere"
    2 Adding the Interval block as replace_where expects an inclusive operator in the where condition
        2.1 If the columns being used to filter in lookback_predicate and incremental_lookback_predicate then we should try to same logic there
        2.2 When the columns being used are not the same, we try to match the statement #1
*/

{% set lookback_predicate %}
    event_date_utc >= {{ get_prev_run_date() }} AND event_date_utc <= {{ get_run_date_w_interval('1 MICROSECONDS') }}
{% endset %}

{% set incremental_lookback_predicate %}
    -- Here "replacement_impression_date_utc" is the same as "event_date_utc"
    replacement_impression_date_utc >= {{ get_prev_run_date() }} AND replacement_impression_date_utc <= {{ get_run_date_w_interval('1 MICROSECONDS') }}
{% endset %}

{{
    config(
        materialized='incremental',
        alias = "fact_event_replacement_impression",
        on_schema_change = "append_new_columns",
        incremental_strategy='replace_where',
        incremental_predicates = [incremental_lookback_predicate]
    )
}}

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
    CAST(NULL AS STRING) AS first_view_item_event_id,
    'N' AS viewed_item_ind,
    CAST(NULL AS TIMESTAMP) AS first_view_item_date_time_utc,
    propensity AS propensity,
    CAST(CURRENT_TIMESTAMP() AS TIMESTAMP) AS dwh_created_date_time_utc
FROM {{ ref('fact_event_replacement_customer') }}
WHERE event_name = 'store.item_card_impressions'

{% if is_incremental() %}
    AND {{ lookback_predicate }}
{% endif %}
