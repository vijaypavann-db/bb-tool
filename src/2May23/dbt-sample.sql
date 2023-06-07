{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='colm',
        snowflake_warehouse=get_snowflake_warehouse('D_WH')
    )
}}

SELECT
    -1 AS rid,
    'cust' AS app,
    event_name AS event_name,
    NULL::VARCHAR AS et_id,
    'N' AS item_ind,
    NULL::DATETIME AS date_time_utc,
    CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9)) AS dtc
FROM {{ ref('custom_base_var') }}
WHERE event_name = 'st.itc'
{% if is_incremental() %}
    AND dutc >= '{{ var('PREV_RUN_DATE_TIME') }}'::DATE
    AND dutc < '{{ var('RUN_DATE_TIME') }}'
{% endif %}
