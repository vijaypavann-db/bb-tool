BEGIN;

DELETE FROM dwh.fact_event_home_engagement
WHERE date_trunc('day',event_date_time_pt) >= DATEADD('DAY',-7,'{start_date}');

INSERT INTO dwh.fact_event_home_engagement (

  SELECT
   name as event_name
   ,data:properties:engagement_type::string engagement_type
   ,data:receivedAt::datetime event_date_time_utc
   ,data:receivedAt::date event_date_utc
   ,CONVERT_TIMEZONE('America/Los_Angeles',data:receivedAt::datetime)::datetime  event_date_time_pt
   ,CONVERT_TIMEZONE('America/Los_Angeles',data:receivedAt::datetime)::date event_date_pt
   ,data:properties:platform::string platform
   ,data:context:app:version::string as app_version
   ,try_cast(data:properties:user_id::string as integer) as user_id
   ,data:properties:ahoy_visit_token::string as visit_token
   ,data:properties:ahoy_visitor_token::string as visitor_token
   ,try_cast(data:properties:warehouse_id::string as integer) warehouse_id
   ,try_cast(data:properties:whitelabel_id::string as integer) whitelabel_id
   ,try_cast(data:properties:country_id::string as integer) as country_id
   ,try_cast(data:properties:inventory_area_id::string as integer) inventory_area_id
   ,try_cast(data:properties:region_id::string as integer) region_id
   ,try_cast(data:properties:zone_id::string as integer) zone_id
   ,data:properties:zip_code::string zip_code
   ,try_cast(data:properties:zip_active::string as BOOLEAN) zip_active
   ,data:properties:service_type::string as service_type
   ,data:properties:home_load_id::string home_load_id
   ,data:properties:section_type::string section_type
   ,data:properties:element_load_id::string element_load_id
   ,data:properties:engagement_details engagement_details
   ,data:properties:section_details as section_details
   ,data:properties:element_type::string as element_type
   ,data:properties:ranking_request_id::string as retailer_ranking_request_id
  FROM
        instadata.events_customers_v2.home h
  WHERE
    h.name in ('home.display','home.engagement')
    AND date_trunc('day',CONVERT_TIMEZONE('America/Los_Angeles',received_at::datetime)::datetime) >= DATEADD('DAY',-7,'{start_date}')
    AND data:properties:platform::string in ('ios','android','web','mobile_web')
    )
;


COMMIT;
