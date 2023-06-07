INSERT INTO FACT_EVENT_CART_IMPRESSION (
	etl_run_id
	,data_source
	,cart_impression_source_file_name
	,cart_impression_event_id
	,page_view_id
	,parent_id
	,cart_id
	,cart_instance_id
	,cart_impression_sent_date_time_utc
	,cart_impression_received_date_time_utc
	,cart_impression_date_time_utc
	,cart_impression_date_utc
	,cart_impression_date_time_pt
	,cart_impression_date_pt
	,cart_item_card_impression_id
	,item_id
	,source_type
	,source_value
	,display_position
	,item_impression_attributes
	,grid_column
	,grid_row
	,dwh_created_date_time_utc
	,dwh_updated_date_time_utc
	,converted_ind
	,product_id
	,warehouse_id
	,region_id
	,whitelabel_id
	,platform
	,visit_token
	,visitor_token
	,user_id
	,ml_engine
	,ml_propensity_score
	,loaded_at
	,full_elastic_item_id
	,ml_score
	)
SELECT {etl_run_id}
	,'event-el' AS data_source
	,null AS cart_impression_source_file_name
	,data: messageId || '-' || nvl(INDEX, 0) AS cart_impression_event_id
	,data: properties.page_view_id AS page_view_id
	,data: properties.parent_id AS parent_id
	,data: properties.cart_id AS cart_id
	,data: properties.cart_instance_id AS cart_instance_id
	,TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR(data: sentAt)) AS cart_impression_sent_date_time_utc
	,TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR(data: receivedAt)) AS cart_impression_received_date_time_utc
	,TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR(data:timestamp)) AS cart_impression_date_time_utc
	,TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR(data:timestamp))::DATE AS cart_impression_date_utc
	,CONVERT_TIMEZONE('UTC', 'US/Pacific', TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR(data:timestamp))) AS cart_impression_date_time_pt
	,CONVERT_TIMEZONE('UTC', 'US/Pacific', TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR(data:timestamp)))::DATE AS cart_impression_date_pt
	,f.value: item_card_impression_id AS cart_item_card_impression_id
	,TRY_TO_NUMBER(TO_VARCHAR(f.value: item_id)) AS item_id
	,data: properties.source_type AS source_type
	,data: properties.source_value AS source_value
	,TRY_TO_NUMBER(TO_VARCHAR(f.value: display_position)) AS display_position
	,f.value:impression_attributes as item_impression_attributes
	,TRY_TO_NUMBER(TO_VARCHAR(f.value: grid_column)) AS grid_column
	,TRY_TO_NUMBER(TO_VARCHAR(f.value: grid_row)) AS grid_row
	,TIMESTAMP AS dwh_created_date_time_utc
	,TIMESTAMP AS dwh_updated_date_time_utc
	,NULL AS converted_ind
	,TRY_TO_NUMBER(TO_VARCHAR(f.value: product_id)) AS product_id
	,TRY_TO_NUMBER(TO_VARCHAR(data: properties.warehouse_id)) AS warehouse_id
	,TRY_TO_NUMBER(TO_VARCHAR(data: properties.region_id)) AS region_id
	,TRY_TO_NUMBER(TO_VARCHAR(data: properties.whitelabel_id)) AS whitelabel_id
	,data: properties.platform AS platform
	,data: properties.ahoy_visit_token AS visit_token
	,data: properties.ahoy_visitor_token AS visitor_token
	,data: userId AS user_id
	,f.value: ml_engine AS ml_engine
	,f.value: ml_propensity_score AS ml_propensity_score
	,loaded_at
	,CASE TYPEOF(f.value:full_elastic_item_id)
                WHEN 'INTEGER' THEN f.value:full_elastic_item_id::integer
                WHEN 'VARCHAR' THEN TRY_TO_NUMBER(REPLACE(f.value:full_elastic_item_id, '''', ''))
                ELSE NULL
              END AS full_elastic_item_id
    ,TRY_CAST(f.value:ml_score::varchar as float) as ml_score
FROM events_customers_v2.cart__item_card_impressions ic
	,LATERAL FLATTEN(INPUT => ic.data, PATH => 'properties.item_card_impressions') f
WHERE loaded_at > (
		SELECT nvl(max(loaded_at), '2019-01-01')
		FROM FACT_EVENT_CART_IMPRESSION
		);