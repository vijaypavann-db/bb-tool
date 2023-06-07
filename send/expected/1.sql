SELECT
  ARRAY_JOIN(
      SLICE(SPLIT(source_file_name, '-'), 5, 8), ''
  ) AS source_file_folder,
  source_file_name,
  data_source,
  data_source_user_tracking_id,
  event_type,
  event_name,
  event_id,
  visit_token,
  visitor_token,
  user_id,
  platform,
  api_version,
  app_version,
  event_date_time_utc,
  event_date_time_pt,
  CAST(event_date_time_utc AS DATE) AS event_date_utc,
  CAST(event_date_time_pt AS DATE) AS event_date_pt,
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
  whitelabel_retailer,
  whitelabel_id,
  wl_exclusive,
  TRY_CAST(inventory_area_id AS DECIMAL(38, 0)) AS inventory_area_id,
  replacements_view_id,
  num_replacements,
  total_replacements,
  item_card_impression_id AS replacement_item_impression_id,
  from_item_modal,
  fetch_error_ind,
  item_id,
  order_id,
  order_delivery_id,
  order_item_id,
  CAST(
      i.display_position AS DECIMAL(38, 0)
  ) AS item_card_impression_display_position,
  CAST(i.replacement_score AS DOUBLE) AS item_card_impression_model_score,
  CAST(
      i.impression_attributes AS STRING
  ) AS item_card_impression_impression_attributes,
  CAST(i.grid_row AS DECIMAL(38, 0)) AS item_card_impression_grid_row,
  CAST(i.item_card_impression_id AS STRING) AS item_card_impression_id,
  CAST(i.grid_column AS DECIMAL(38, 0)) AS item_card_impression_grid_column,
  CAST(i.item_id AS DECIMAL(38, 0)) AS item_card_impressions_item_id,
  CAST(CURRENT_TIMESTAMP() AS TIMESTAMP) AS dwh_created_date_time_utc,
  CAST(i.propensity AS FLOAT) AS propensity,
  candidates
FROM
    dwh.vw_replacement_customer
LATERAL VIEW OUTER EXPLODE(item_card_impressions) AS i
WHERE
    event_date_pt = '{start_date}' AND
    (
        event_name IN ('store.replacements_view',
                    'store.select_replacement',
                    'store.view_replacements',
                    'store.reject_replacement',
                    'store.view_more_replacements'
                )
    OR (
        event_name IN ('store.item_card_impressions')
        AND source_type IN ('checkout_replacements',
                            'post_checkout_replacements',
                            'order_item_changes',
                            'low_stock',
                            'low_stock_more_options',
                            'low_stock_more_options_search',
                            'high_risk_pair',
                            'high_risk_more_options',
                            'high_risk_more_options_search')
        )
    )