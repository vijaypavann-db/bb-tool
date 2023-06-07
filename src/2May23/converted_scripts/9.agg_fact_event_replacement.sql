BEGIN TRANSACTION;

DELETE FROM dwh.fact_event_replacement_impression where replacement_impression_date_time_utc::DATE >= '{start_date}'
and replacement_impression_date_time_utc::DATE < '{end_date}';

INSERT INTO dwh.fact_event_replacement_impression (
  etl_run_id
  ,app
  ,event_name
  ,replacement_impression_event_id
  ,replacements_view_id
  ,replacement_impression_sent_date_time_utc
  ,replacement_impression_received_date_time_utc
  ,replacement_impression_date_time_utc
  ,replacement_impression_date_utc
  ,replacement_impression_date_time_pt
  ,replacement_impression_date_pt
  ,source_type
  ,replacement_item_impression_id
  ,replacement_item_impression_type
  ,item_id
  ,model_score
  ,display_position
  ,item_impression_attributes
  ,grid_column
  ,grid_row
  ,first_view_item_event_id
  ,viewed_item_ind
  ,first_view_item_date_time_utc
  ,propensity
)
SELECT
  -1
  ,'customer'
  ,f1.event_name
  ,f1.event_id
  ,f1.replacements_view_id
  ,f1.event_sent_date_time_utc
  ,f1.event_received_date_time_utc
  ,f1.event_date_time_utc
  ,f1.event_date_utc
  ,f1.event_date_time_pt
  ,f1.event_date_pt
  ,f1.source_type
  ,f1.item_card_impression_id
  ,'item_card'
  ,item_card_impressions_item_id
  ,item_card_impression_model_score
  ,item_card_impression_display_position
  ,item_card_impression_impression_attributes
  ,item_card_impression_grid_column
  ,item_card_impression_grid_row
  ,null
  ,'N'
  ,null
  ,propensity
FROM fact_event_replacement_customer f1
WHERE  event_date_time_utc::DATE >='{start_date}' AND event_date_time_utc::DATE < '{end_date}'
AND
f1.event_name = 'store.item_card_impressions';
-- Commenting out the insertion from shopper data since there are no records for replacements.item_card_impressions in the fact_event_replacement_shopper table since 2020 July.
-- I_F doing backfill for data before 2020 July, please add the following commented out code.
--INSERT INTO dwh.fact_event_replacement_impression (
--  etl_run_id
--  ,app
--  ,event_name
--  ,replacement_impression_event_id
--  ,replacements_view_id
--  ,replacement_impression_sent_date_time_utc
--  ,replacement_impression_received_date_time_utc
--  ,replacement_impression_date_time_utc
--  ,replacement_impression_date_utc
--  ,replacement_impression_date_time_pt
--  ,replacement_impression_date_pt
--  ,source_type
--  ,replacement_item_impression_id
--  ,replacement_item_impression_type
--  ,item_id
--  ,model_score
--  ,display_position
--  ,item_impression_attributes
--  ,grid_column
--  ,grid_row
--  ,first_view_item_event_id
--  ,viewed_item_ind
--  ,first_view_item_date_time_utc
--  ,propensity
--)
--SELECT
--  -1
--  ,'shopper'
--  ,f1.event_name
--  ,f1.event_id
--  ,f1.replacements_view_id
--  ,f1.event_sent_date_time_utc
--  ,f1.event_received_date_time_utc
--  ,f1.event_date_time_utc
--  ,f1.event_date_utc
--  ,f1.event_date_time_pt
--  ,f1.event_date_pt
--  ,f1.source_type
--  ,f1.item_card_impression_id
--  ,'item_card'
--  ,item_card_impressions_item_id
--  ,item_card_impression_model_score
--  ,item_card_impression_display_position
--  ,item_card_impression_impression_attributes
--  ,item_card_impression_grid_column
--  ,item_card_impression_grid_row
--  ,null
--  ,'N'
--  ,null
--  ,null
--FROM fact_event_replacement_shopper f1
--WHERE  event_date_time_utc::DATE >='{start_date}' AND event_date_time_utc::DATE < '{end_date}'
--AND
--f1.event_name = 'replacements.item_card_impressions'
-- -- -- -- -- -- -- Conversion -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

INSERT OVERWRITE dwh.fact_event_replacement_conversion (
  etl_run_id
  ,app
  ,event_name
  ,replacement_conversion_event_id
  ,replacement_conversion_sent_date_time_utc
  ,replacement_conversion_received_date_time_utc
  ,replacement_converted_date_time_utc         
  ,replacement_converted_date_utc             
  ,replacement_converted_date_time_pt        
  ,replacement_converted_date_pt            
  ,replacements_view_id                    
  ,replacement_item_impression_id        
  ,replacement_item_impression_type        
  ,source_type
  ,original_item_id                        
  ,converted_item_id                       
  ,conversion_source
  ,order_item_id
)
SELECT 
  -1
 ,'customer'
 ,f1.event_name
 ,f1.event_id
 ,f1.event_sent_date_time_utc
 ,f1.event_received_date_time_utc
 ,f1.event_date_time_utc
 ,f1.event_date_utc
 ,f1.event_date_time_pt
 ,f1.event_date_pt
 ,f2.replacements_view_id
 ,f1.replacement_item_impression_id
 ,'item_card'
 ,f1.source_type
  ,coalesce( try_to_number(f1.source_value),-1)
 ,f1.item_id
 ,CASE WHEN f1.from_item_modal = 'Y' THEN 'item_modal' ELSE 'replacements_page' END
 ,f1.order_item_id
FROM fact_event_replacement_customer f1
LEFT JOIN dwh.fact_event_replacement_impression f2
  ON f1.replacement_item_impression_id = f2.replacement_item_impression_id
 AND f2.app = 'customer'
 AND f2.event_name = 'store.item_card_impressions'
 AND (f1.replacements_view_id != f1.order_item_id OR datediff('seconds', f1.event_date_time_utc, f2.replacement_impression_date_time_utc) between -120 and 120)
WHERE f1.event_name in ( 'store.select_replacement' , 'store.reject_replacement')
AND   lower(f1.source_type) NOT IN ('search', 'order_changes_replacements');
INSERT INTO dwh.fact_event_replacement_conversion (
  etl_run_id
  ,app
  ,event_name
  ,replacement_conversion_event_id
  ,replacement_conversion_sent_date_time_utc
  ,replacement_conversion_received_date_time_utc
  ,replacement_converted_date_time_utc
  ,replacement_converted_date_utc
  ,replacement_converted_date_time_pt
  ,replacement_converted_date_pt
  ,replacements_view_id
  ,replacement_item_impression_id
  ,replacement_item_impression_type
  ,source_type
  ,original_item_id
  ,converted_item_id
  ,conversion_source
  ,order_item_id
)
SELECT
  -1
 ,'shopper'
 ,f1.event_name
 ,f1.event_id
 ,f1.event_sent_date_time_utc
 ,f1.event_received_date_time_utc
 ,f1.event_date_time_utc
 ,f1.event_date_utc
 ,f1.event_date_time_pt
 ,f1.event_date_pt
 ,f2.replacements_view_id
 ,f1.replacement_item_impression_id
 ,'item_card'
 ,f1.source_type
  ,coalesce( try_to_number(f1.source_value),-1)
 ,f1.item_id
 ,CASE WHEN f1.from_item_modal = 'Y' THEN 'item_modal' ELSE 'replacements_page' END
 ,f1.order_item_id
FROM fact_event_replacement_shopper f1
JOIN fact_event_replacement_impression f2
  ON f1.replacement_item_impression_id = f2.replacement_item_impression_id
 AND f2.app = 'shopper'
 AND f2.event_name = 'replacements.item_card_impressions'
WHERE f1.event_name = 'replacements.select_replacement'
AND   f1.source_type != 'search';
INSERT INTO dwh.fact_event_replacement_conversion (
  etl_run_id
  ,app
  ,event_name
  ,replacement_conversion_event_id
  ,replacement_conversion_sent_date_time_utc
  ,replacement_conversion_received_date_time_utc
  ,replacement_converted_date_time_utc
  ,replacement_converted_date_utc
  ,replacement_converted_date_time_pt
  ,replacement_converted_date_pt
  ,replacements_view_id
  ,replacement_item_impression_id
  ,replacement_item_impression_type
  ,source_type
  ,original_item_id
  ,converted_item_id
  ,conversion_source
  ,order_item_id
)
SELECT
  -1
 ,'shopper'
 ,f1.event_name
 ,f1.event_id
 ,f1.event_sent_date_time_utc
 ,f1.event_received_date_time_utc
 ,f1.event_date_time_utc
 ,f1.event_date_utc
 ,f1.event_date_time_pt
 ,f1.event_date_pt
 ,NULL
 ,f1.replacement_item_impression_id
 ,'item_card'
 ,f1.source_type
  ,coalesce( try_to_number(f1.source_value),-1)
 ,f1.item_id
 ,CASE WHEN f1.from_item_modal = 'Y' THEN 'item_modal' ELSE 'replacements_page' END
 ,f1.order_item_id
FROM fact_event_replacement_shopper f1
WHERE f1.event_name = 'replacements.select_replacement'
AND   NVL(f1.replacement_item_impression_id, '-100') NOT IN (
   		SELECT replacement_item_impression_id FROM fact_event_replacement_impression
   		WHERE  app = 'shopper' AND replacement_item_impression_id is not null);
-- -- -- -- -- -- -- View -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

INSERT OVERWRITE dwh.fact_event_replacement_view (
 	etl_run_id
     ,app       
     ,event_name 
     ,replacement_view_event_id 
     ,replacement_view_event_sent_date_time_utc
     ,replacement_view_event_received_date_time_utc
     ,replacement_view_event_date_time_utc
     ,replacement_view_event_date_utc
     ,replacement_view_event_date_time_pt
     ,replacement_view_event_date_pt
     ,replacements_view_id
     ,parent_id
     ,num_replacements
     ,total_replacements
     ,platform
     ,source_type
     ,source_value
     ,exclude_ind 
     ,fetch_error_ind
     ,user_id
     ,shopper_id
     ,shopper_roles
     ,shopper_shift_id
     ,shift_type
     ,order_item_id
     ,order_delivery_id
     ,order_id
     ,api_version
     ,app_version
     ,warehouse_id
     ,inventory_area_id
     ,zone_id
     ,zip_code
     ,whitelabel_retailer
     ,whitelabel_ind
     ,wl_exclusive
     ,country_id
     ,converted_ind
     ,last_replacement_conversion_event_id
     ,last_replacement_conversion_date_time_utc
     ,candidates
)
WITH rc AS (
  SELECT replacement_conversion_event_id,
 		event_name,
 		replacement_converted_date_time_utc,
 		replacements_view_id,
 		ROW_NUMBER() OVER (PARTITION BY replacements_view_id ORDER BY replacement_converted_date_time_utc DESC) rank 
  FROM dwh.fact_event_replacement_conversion 
  WHERE app = 'customer'
  AND   event_name in ('store.select_replacement', 'store.reject_replacement')
)
SELECT 
  -1
 ,'customer'
 ,CASE WHEN lower(f1.event_name) = 'store.replacements_view' THEN 'store.view_replacements'
   	ELSE lower(f1.event_name)
  END
 ,f1.event_id
 ,f1.event_sent_date_time_utc
 ,f1.event_received_date_time_utc
 ,f1.event_date_time_utc
 ,f1.event_date_utc
 ,f1.event_date_time_pt
 ,f1.event_date_pt
 ,f1.replacements_view_id
 ,f1.parent_id
 ,f1.num_replacements 
 ,f1.total_replacements
 ,f1.platform
 ,CASE WHEN lower(f1.source_type) = 'high_confidence_replacement' THEN 'high_confidence_replacements'
   	WHEN lower(f1.source_type) = 'user_choice_replacements' THEN 'user_chosen_replacements'
   	ELSE lower(f1.source_type)
  END
,coalesce( try_to_number(f1.source_value),-1)
 ,'N'
 ,NVL(fetch_error_ind, 'N')
 ,coalesce( try_to_number(user_id),-1)
 ,NULL
 ,NULL
 ,NULL
 ,NULL
 ,order_item_id
 ,order_delivery_id
 ,order_id
 ,CAST(api_version AS INT)
 ,app_version
 ,warehouse_id
 ,inventory_area_id
 ,zone_id
 ,zip_code
 ,whitelabel_retailer
 ,CASE WHEN nvl(whitelabel_id, 1) <> 1 THEN 'Y' ELSE 'N' END
 ,wl_exclusive
 ,country_id
 ,CASE WHEN rc.rank = 1 and rc.event_name in ('store.select_replacement') THEN 'Y' ELSE 'N' END as converted_ind
 ,rc.replacement_conversion_event_id
 ,rc.replacement_converted_date_time_utc
 ,f1.candidates
FROM fact_event_replacement_customer f1
LEFT JOIN rc
 ON f1.replacements_view_id = rc.replacements_view_id
AND rc.rank = 1
AND (f1.replacements_view_id != f1.order_item_id OR datediff('seconds', f1.event_date_time_utc, rc.replacement_converted_date_time_utc) between -120 and 120)
WHERE f1.event_name in ('store.replacements_view', 'store.view_replacements')
AND   f1.source_type != 'search';
INSERT INTO dwh.fact_event_replacement_view (
 	etl_run_id
     ,app      
     ,event_name
     ,replacement_view_event_id
     ,replacement_view_event_sent_date_time_utc
     ,replacement_view_event_received_date_time_utc
     ,replacement_view_event_date_time_utc
     ,replacement_view_event_date_utc
     ,replacement_view_event_date_time_pt
     ,replacement_view_event_date_pt
     ,replacements_view_id
     ,parent_id
     ,num_replacements
     ,total_replacements
     ,platform
     ,source_type
     ,source_value
     ,search_term
     ,exclude_ind
     ,fetch_error_ind
     ,user_id
     ,shopper_id
     ,shopper_roles
     ,shopper_shift_id
     ,shift_type
     ,batch_id
     ,order_item_id
     ,order_delivery_id
     ,order_id
     ,api_version
     ,app_version
     ,warehouse_id
     ,inventory_area_id
     ,zone_id
     ,zip_code
     ,whitelabel_retailer
     ,whitelabel_ind
     ,wl_exclusive
     ,country_id
     ,converted_ind
     ,last_replacement_conversion_event_id
     ,last_replacement_conversion_date_time_utc
     ,candidates
)
WITH rc AS (
  SELECT replacement_conversion_event_id,
 		replacement_converted_date_time_utc,
 		replacements_view_id,
 		ROW_NUMBER() OVER (PARTITION BY replacements_view_id ORDER BY replacement_converted_date_time_utc DESC) rank
  FROM dwh.fact_event_replacement_conversion
  WHERE app = 'shopper'
  AND   event_name = 'replacements.select_replacement'
)
SELECT
  -1
 ,'shopper'
 ,CASE WHEN lower(f1.event_name) = 'replacements.replacements_view' THEN 'replacements.view_replacements'
   	ELSE lower(f1.event_name)
  END
 ,f1.event_id
 ,f1.event_sent_date_time_utc
 ,f1.event_received_date_time_utc
 ,f1.event_date_time_utc
 ,f1.event_date_utc
 ,f1.event_date_time_pt
 ,f1.event_date_pt
 ,f1.replacements_view_id
 ,f1.parent_id
 ,f1.num_replacements
 ,f1.total_replacements
 ,f1.platform
 ,CASE WHEN lower(f1.source_type) = 'high_confidence_replacement' THEN 'high_confidence_replacements'
   	WHEN lower(f1.source_type) = 'user_choice_replacements' THEN 'user_chosen_replacements'
   	ELSE lower(f1.source_type)
  END
 ,coalesce( try_to_number(f1.source_value),-1)
 ,f1.search_term
 ,'N'
 ,NVL(fetch_error_ind, 'N')
 ,coalesce( try_to_number(user_id),-1)
 ,shopper_id
 ,shopper_roles
 ,shopper_shift_id
 ,shift_type
 ,batch_id
 ,order_item_id
 ,order_delivery_id
 ,order_id
 ,NULL
 ,app_version
 ,warehouse_id
 ,inventory_area_id
 ,zone_id
 ,zip_code
 ,NULL
 ,NULL
 ,NULL
 ,country_id
 ,CASE WHEN rc.rank = 1 THEN 'Y' ELSE 'N' END
 ,rc.replacement_conversion_event_id
 ,rc.replacement_converted_date_time_utc
 ,NULL
FROM fact_event_replacement_shopper f1
LEFT JOIN rc
 ON f1.replacements_view_id = rc.replacements_view_id
AND rc.rank = 1
WHERE f1.event_name in ('replacements.replacements_view', 'replacements.view_replacements');
COMMIT;