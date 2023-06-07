CREATE OR REPLACE TEMPORARY TABLE mvw_fact_event_category_hero AS
SELECT to_char(current_timestamp(), 'yyyymmhh24miss') AS etl_run_id
	,'segment' data_source
	,NULL AS source_file_id
	,NULL AS raw_data_id
	,NULL AS file_name
	,CASE
		WHEN LENGTH(REPLACE(d.data:messageId, '\u0000', '')) > 256 THEN SUBSTRING(REPLACE(d.data:messageId, '\u0000', ''), 1, 253) || '...'
		ELSE DECODE(REPLACE(d.data:messageId, '\u0000', ''), 'null', NULL, REPLACE(d.data:messageId, '\u0000', ''))
	END AS event_id
	,'track' AS event_type
	,name AS event_name
	,TRY_TO_TIMESTAMP(CASE
          WHEN ENDSWITH(d.data:sentAt, 'Z') THEN REPLACE(d.data:sentAt,'>', ':')
          ELSE ''
          END)::TIMESTAMP_NTZ  AS event_sent_date_time_utc
	,TRY_TO_TIMESTAMP(d.data:receivedAt::VARCHAR)::TIMESTAMP_NTZ  AS event_received_date_time_utc
	,REPLACE(d.data:timestamp,'>', ':')::TIMESTAMP_NTZ  AS event_date_time_utc
	,TO_DATE(REPLACE(d.data:timestamp,'>', ':')::TIMESTAMP_NTZ) AS event_date_utc
	,CONVERT_TIMEZONE('UTC', 'US/Pacific', REPLACE(d.data:timestamp,'>', ':')::TIMESTAMP_NTZ) AS event_date_time_pt
	,to_date(event_date_time_pt) AS event_date_pt
	,REPLACE(d.data:anonymousId, '\u0000', '') AS data_source_user_tracking_id
	,DECODE(data:userId, 'null', NULL, data:userId) AS user_id
	,DECODE(data:properties.aisle, 'null', NULL, data:properties.aisle) AS aisle_name
	,DECODE(data:properties.category_hero_id, 'null', NULL, data:properties.category_hero_id) AS category_hero_id
	,DECODE(data:properties.department, 'null', NULL, data:properties.department) AS department_name
	,DECODE(data:properties.page_type, 'null', NULL, data:properties.page_type) AS page_type
	,DECODE(data:properties.search_term, 'null', NULL, SUBSTRING(data:properties.search_term, 1, 512)) AS search_term
	,DECODE(data:properties.hero_position, 'null', NULL, data:properties.hero_position) AS hero_position
	,DECODE(data:properties.page_view_id, 'null', NULL, data:properties.page_view_id) AS page_view_id
	,d.loaded_at
FROM  events_customers_v2.category_hero d
WHERE
        d.loaded_at  >= (select nvl(dateadd('hour', -1, max(loaded_at)), dateadd('hour', -6, current_timestamp())) from dwh.fact_event_category_hero)
        and REPLACE(d.data:messageId, '\u0000', '')not in (select event_id from dwh.fact_event_category_hero
                                                             where dwh_created_date_time_utc > dateadd(hour, -6, current_timestamp)) ;


INSERT INTO dwh.fact_event_category_hero (
	etl_run_id
	,data_source
	,source_file_id
	,file_name
	,raw_data_id
	,event_id
	,event_type
	,event_name
	,event_sent_date_time_utc
	,event_received_date_time_utc
	,event_date_time_utc
	,event_date_utc
	,event_date_time_pt
	,event_date_pt
	,data_source_user_tracking_id
	,user_id
	,aisle_name
	,category_hero_id
	,department_name
	,page_type
	,search_term
	,hero_position
	,page_view_id
	,loaded_at
	)
SELECT etl_run_id
	,data_source
	,source_file_id
	,file_name
	,raw_data_id
	,event_id
	,event_type
	,event_name
	,event_sent_date_time_utc
	,event_received_date_time_utc
	,event_date_time_utc
	,event_date_utc
	,event_date_time_pt
	,event_date_pt
	,data_source_user_tracking_id
	,user_id
	,aisle_name
	,category_hero_id
	,department_name
	,page_type
	,search_term
	,hero_position
	,page_view_id
	,loaded_at
FROM mvw_fact_event_category_hero;

COMMIT;
