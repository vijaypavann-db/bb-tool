
INSERT OVERWRITE DWH.FACT_EVENT_REPLACEMENT_SHOPPER PARTITION(event_date_pt = '{start_date}') 
(
	source_file_folder  ,
	source_file_name  ,
	data_source  ,
	data_source_user_tracking_id  ,
	event_type  ,
	event_name  ,
	event_id  ,
	user_id  ,
	shopper_id  ,
	shopper_roles  ,
	shopper_shift_id  ,
	shift_type  ,
	batch_id  ,
	platform  ,
	app_version  ,
	event_date_time_utc  ,
	event_date_time_pt  ,
	event_date_utc ,
	event_date_pt ,
	event_sent_date_time_utc  ,
	event_received_date_time_utc  ,
	source_type  ,
	source_value,
	parent_id  ,
	country_id  ,
	currency  ,
	zip_code  ,
	zone_id  ,
	warehouse_id  ,
	inventory_area_id  ,
	replacements_view_id  ,
	order_id  ,
	order_delivery_id  ,
	order_item_id  ,
	num_replacements  ,
	total_replacements  ,
	replacement_item_impression_id  ,
	from_item_modal  ,
	fetch_error_ind  ,
	item_id  ,
	item_card_impression_display_position  ,
	item_card_impression_model_score  ,
	item_card_impression_impression_attributes  ,
	item_card_impression_grid_row  ,
	item_card_impression_id  ,
	item_card_impression_grid_column  ,
	item_card_impressions_item_id  ,
	dwh_created_date_time_utc  ,
	search_term )
-- FIXME. databricks.migrations.unsupported  Named  paramaters in function call
SELECT
	CASE WHEN data_source = 'segment' THEN dateadd(ms, split_part(source_file_name, '.', 1), '1970-01-01')::date::STRING ELSE source_file_name END as source_file_folder,
	source_file_name  ,
	data_source  ,
	data_source_user_tracking_id  ,
	event_type  ,
	event_name  ,
	event_id  ,
	user_id  ,
	shopper_id  ,
	shopper_roles  ,
	shopper_shift_id  ,
	shift_type  ,
	batch_id  ,
	platform  ,
	app_version  ,
	event_date_time_utc  ,
	event_date_time_pt  ,
	event_date_time_utc::date as event_date_utc,
	event_date_time_pt::date as event_date_pt,
	event_sent_date_time_utc  ,
	event_received_date_time_utc  ,
	source_type  ,
	source_value,
	parent_id  ,
	country_id  ,
	currency  ,
	zip_code  ,
	zone_id  ,
	warehouse_id  ,
	inventory_area_id  ,
	replacements_view_id  ,
	order_id  ,
	order_delivery_id  ,
	order_item_id  ,
	num_replacements  ,
	total_replacements  ,
	replacement_item_impression_id  ,
	from_item_modal  ,
	fetch_error_ind  ,
	item_id  ,
	TRY_TO_NUMBER(i.display_position::STRING) as item_card_impression_display_position  ,
	TRY_CAST(i.model_score::STRING as NUMERIC) as item_card_impression_model_score  ,
	i.impression_attributes::STRING as item_card_impression_impression_attributes,
	TRY_TO_NUMBER(i.grid_row::STRING) as item_card_impression_grid_row  ,
	i.item_card_impression_id::STRING as item_card_impression_id  ,
	TRY_TO_NUMBER(i.grid_column::STRING) as item_card_impression_grid_column  ,
	TRY_TO_NUMBER(i.item_id::STRING) as item_card_impressions_item_id  ,
	cast(current_timestamp() as timestamp_ntz(9)) as dwh_created_date_time_utc,
	search_term
FROM
dwh.vw_replacement_shopper d,
-- FIXME databricks.migration.unsupported.function LATERAL_FLATTEN
LATERAL VIEW EXPLODER_OUTER(INPUT => d.item_card_impressions) i
WHERE event_date_pt = '{start_date}'
AND event_name in ('replacements.replacements_view', 'replacements.select_replacement', 'replacements.view_replacements', 'replacements.item_card_impressions');
