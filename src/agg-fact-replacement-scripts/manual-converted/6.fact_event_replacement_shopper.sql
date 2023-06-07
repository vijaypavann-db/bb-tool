/* P.S:
    1. All the data returned by lookback_predicate has to satisfy the condition in incremental_lookback_predicate (i.e. the replace where clause), otherwise the insert will fail.
        with Error:  "Data written out does not match replaceWhere"
    2 Adding the Interval block as replace_where expects an inclusive operator in the where condition
        2.1 If the columns being used to filter in lookback_predicate and incremental_lookback_predicate then we should try to same logic there
        2.2 When the columns being used are not the same, we try to match the statement #1
*/

{% set lookback_predicate %}
    event_date_pt = {{ get_run_date() }} AND dt >= {{ get_prev_run_date() }}
{% endset %}

{% set incremental_lookback_predicate %}
    event_date_pt = {{ get_run_date() }}
{% endset %}

{{
  config(
          materialized="incremental",
          alias= "fact_event_replacement_shopper",
          on_schema_change = "append_new_columns",
          incremental_strategy="replace_where",
          incremental_predicates=[incremental_lookback_predicate]
      )
}}

SELECT
    data_source,
    data_source_user_tracking_id,
    event_type,
    event_name,
    event_id,
    user_id,
    shopper_id,
    shopper_roles,
    shopper_shift_id,
    shift_type,
    batch_id,
    platform,
    app_version,
    event_date_time_utc,
    event_date_time_pt,
    event_date_utc,
    event_date_pt,
    event_sent_date_time_utc,
    event_received_date_time_utc,
    source_type,
    source_value,
    parent_id,
    country_id,
    currency,
    zip_code,
    zone_id,
    warehouse_id,
    inventory_area_id,
    replacements_view_id,
    order_id,
    order_delivery_id,
    order_item_id,
    num_replacements,
    total_replacements,
    replacement_item_impression_id,
    from_item_modal,
    fetch_error_ind,
    item_id,
    CAST(NULL AS STRING) AS item_card_impression_display_position,
    CAST(NULL AS STRING) AS item_card_impression_model_score,
    CAST(NULL AS STRING) AS item_card_impression_impression_attributes,
    CAST(NULL AS STRING) AS item_card_impression_grid_row,
    CAST(NULL AS STRING) AS item_card_impression_id,
    CAST(NULL AS STRING) AS item_card_impression_grid_column,
    CAST(NULL AS STRING) AS item_card_impressions_item_id,
    CURRENT_TIMESTAMP() AS dwh_created_date_time_utc,
    CAST(NULL AS STRING) AS search_term
FROM
{{ ref('vw_replacement_shopper') }}
WHERE
    dt >={{ get_run_date_w_interval('2 days') }}
    AND
{{ lookback_predicate }}
