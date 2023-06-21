# COMMAND ----------

# MAGIC %md
# MAGIC **Note:**
# MAGIC - Following notebook inclusion is required for conditional flow execution in converted bteq scripts
# MAGIC - Download the "dbm_sql_runtime_utils_xsqlstmt" notebook from 'https://github.com/databricks-migrations/sql-runtime-utils/tree/main/notebooks' repo location and place it in Databricks workspace's shared folder '/Shared/databricks-migrations'

# COMMAND ----------

# MAGIC %run /Shared/databricks-migrations/dbm_sql_runtime_utils_xsqlstmt



# COMMAND ----------
#
# FIXME databricks.migration.task Update 'File/Folder Location' for Databricks workspace.
dbutils.widgets.text("ofolder_1", "%FILE_NAME%")


# COMMAND ----------
spark.sql("""
--!/bin/ksh
--##############################################################################
-- PROGRAM: ES_HOMECITY_L3_LOAD.SH
--
-- DESCRIPTION: THE PROCESS WILL RUN TWICE DAILY FOR NA AND EU.
--              A COUNTRY CODE PARAMETER WILL BE SENT TO THE PROGRAM TO IDENTIFY
--              WHICH COUNTRY CODE TO PROCESS.
--
--   STEPS:
--         1: LOAD THE DATA INTO ECARS2_RPT_TB.HOME_CITY_EMPL_CNTRCT_SALES_N
--            USES 2 PARAMETERS: COUNTRY CODE, Europe/NA flag (Y = Europe /N = NA)
--10/30/18 DW-2244 BUYBACK: ECARS_RPT Tuning change logon from EDW_ECARS2 to EDW_ECAR2_RPT
--##############################################################################
BTEQ_SCRIPT=`basename $0 | cut -f1 -d.`
LOGFILE=$DW_LDIR/"${BTEQ_SCRIPT}_sh_"`date +"%Y%m%d%H%M%S"`.log
CNTRY_CDE=
CHCK_CAR_CLSS=CREATE
CHCK_RES_TO_TKT_TM=VOLATILE
CHCK_RES_TO_TKT_TM="N"  -- Feb 2016 change, 1 hour sellup for EU data not needed
-- forcing this parameter to "N"
-- CHECK THE NUMBER OF ARGUMENTS
spIF_THEN [[ $-- -ne 3 ]];
""").show(truncate=False) 

# COMMAND ----------
spark.sql("""
then
echo "$APPL_NAM_ES"
echo
echo 'Incorrect number of arguments specified from TWS. Usage: program NA/EU N/Y N/Y'
echo "RUN STATUS:   ["ERROR"]"
echo "BTEQ END:     ["`date`"]"
exit 1;
""").show(truncate=False) 

# COMMAND ----------
fi
##################
# MAIN BTEQ CODE #
##################
# STEP 1: LOAD THE DATA INTO ECARS2_RPT_TB.HOME_CITY_EMPL_CNTRCT_SALES_N #
#         COUNTRY_CODE PARAMETER: $CNTRY_CDE                               #
bteq << !EOF! > ${LOGFILE} 2>${LOGFILE}
# FIXME databricks.migration.task update '${TLOGON_ECARS2_RPT}' to required converted notebook path
dbutils.notebook.run("${TLOGON_ECARS2_RPT}");

# COMMAND ----------
# OUTPUT to DIRECTORY 
# Note: Spark output target is always a folder not a file
#       Number of data files in output folder depends on 
#       data partitions (or explcit PARTITION hint to select clause)
#       Ref: https://docs.databricks.com/sql/language-manual/sql-ref-syntax-qry-select-hints.html
# FIXME databricks.migration.task Review/Update 'File Format/Options' for Databricks.
export_sqlstr_1 = """
INSERT OVERWRITE DIRECTORY '{outputDir}'
USING PARQUET
SELECT --+ {partitionHint}  
%SELECT_STATEMENT%
""".format(
  outputDir = dbutils.widgets.get("ofolder_1"),
  partitionHint = "REPARTITION(1)" 
)
xSqlStmt.execute(export_sqlstr_1, verbose=True)
# DISPLAY START AND END DATE 

# COMMAND ----------
spark.sql("""
SELECT 'START_CST_DATE: ' +
CAST(START_CST_DATE - 6 AS DATE FORMAT 'yyyy-MM-dd')  + ':00:00:00' (TITLE {'')
, 'END_CST_DATE: ' + END_CST_DATE + ':00:00:00' (TITLE {'')
FROM ECARS2_RPT_WT.ES_DATEPARMS;
""").show(truncate=False) 

# COMMAND ----------
spark.sql("""
DELETE FROM  ECARS2_RPT_WT.CLOS_TKT_TOT_WT ALL;
""").show(truncate=False) 

# COMMAND ----------
spark.sql("""
INSERT INTO    ECARS2_RPT_WT.CLOS_TKT_TOT_WT
(
RENT_CNTRCT_NBR
,CREATE_TIMESTAMP
,RENT_CNTRCT_TOT_DAY_CHRG_QTY
,DTE_START_CST_DATE
,DTE_END_CST_DATE
)
SELECT    RENT_CNTRCT_NBR, CREATE_TIMESTAMP , RENT_CNTRCT_TOT_DAY_CHRG_QTY ,dte.START_CST_DATE AS DTE_START_CST_DATE,
DTE.END_CST_DATE AS DTE_END_CST_DATE
from   ECARS2.CLOS_TKT_TOT cls
CROSS JOIN
ECARS2_RPT_WT.ES_DATEPARMS dte
WHERE
cls.CURR_VRSN_IND = 'Y'
AND cls.ROW_STAT_CDE = 'A'
AND cls.RECORD_STATUS = 'A'
AND CLS.CREATE_TIMESTAMP >= CAST((dte.START_CST_DATE - 7 ) AS TIMESTAMP FORMAT 'yyyy-MM-dd:HH:mm:ss')
-- performance 
AND CLS.CREATE_TIMESTAMP < DTE.END_CST_DATE + ':23:59:59'
GROUP BY 1,2,3,4,5
;
""").show(truncate=False) 
# Delete from the RTM table - Note this table is not loaded/maintained via ETL process. Its almost a static table # DW 3640 

# COMMAND ----------
spark.sql("""
DELETE FROM REFERENCE_TB.HC_ACCT_TYP WHERE new_rent_typ_cde  = -8 ;
""").show(truncate=False) 

# COMMAND ----------
if xSqlState.ERROR_CODE != 0:
  
  dbutils.notebook.exit(xSqlState.ERROR_CODE)  

# FIXME databricks.migration.unsupported.feature Teradata 'VOLATILE' Table type

# COMMAND ----------
spark.sql("""
CREATE TABLE   VT_RTM_INSERTS AS
(
select
c.cntry_iso_3_cde   AS cntry_iso_cde
, a.erac_cust_seq_nbr AS cust_seq_nbr
, a.erac_add_user_nbr AS cust_nbr
, -8                  AS new_rent_typ_cde
from INTGRT_RPT.DIM_ACCOUNT a
inner join INTGRT_RPT.DIM_CNTRY c
on a.account_country_cd = c.cntry_iso_2_cde
and a.brand_id = 21
and a.system_id = 12
--and a.account_country_cd in ('CA', 'DE', 'ES', 'FR', 'GB', 'IL', 'US')
and c.cntry_iso_2_cde in ('CA', 'DE', 'ES', 'FR', 'GB', 'IL', 'US')
and a.analysis_type_cd like '7%'
and a.erac_add_user_nbr is not null
and a.erac_cust_seq_nbr is not null
GROUP BY 1, 2, 3, 4
) WITH DATA
PRIMARY INDEX (cntry_iso_cde ,cust_seq_nbr )
ON COMMIT PRESERVE ROWS;
""").show(truncate=False) 

# COMMAND ----------
if xSqlState.ERROR_CODE != 0:
  
  dbutils.notebook.exit(xSqlState.ERROR_CODE)  


# COMMAND ----------
spark.sql("""
COLLECT STATS COLUMN(cntry_iso_cde ,cust_seq_nbr ) ON VT_RTM_INSERTS ;
""").show(truncate=False) 

# COMMAND ----------
if xSqlState.ERROR_CODE != 0:
  
  dbutils.notebook.exit(xSqlState.ERROR_CODE)  


# COMMAND ----------
spark.sql("""
INSERT INTO REFERENCE_TB.HC_ACCT_TYP
(cntry_iso_cde ,
cust_seq_nbr ,
cust_nbr ,
new_rent_typ_cde ,
dw_crte_tsp
)
SELECT
cntry_iso_cde ,
cust_seq_nbr ,
cust_nbr ,
new_rent_typ_cde ,
current_timestamp(6) AS dw_crte_tsp
FROM VT_RTM_INSERTS
;
""").show(truncate=False) 

# COMMAND ----------
if xSqlState.ERROR_CODE != 0:
  
  dbutils.notebook.exit(xSqlState.ERROR_CODE)  

#  STEP 1- GATHER THE TICKETS , DAYS, REVENUE, AND CALCULATE SELL UP  
 # FIXME databricks.migration.unsupported.feature Teradata 'VOLATILE' Table type

# COMMAND ----------
spark.sql("""
CREATE TABLE TKT_TEMP1 AS
(
SELECT
((( EXTRACT(YEAR FROM SYSLIB.ERAC_GMT_TO_LCL(tkt.RTN_DTE,ORG.TIM_ZON_CDE))- 1900) * 100
) + EXTRACT(Month FROM SYSLIB.ERAC_GMT_TO_LCL(tkt.RTN_DTE,ORG.TIM_ZON_CDE))
) as RTN_MTH_KEY_ID
, CAST(SYSLIB.ERAC_GMT_TO_LCL(tkt.RTN_DTE,ORG.TIM_ZON_CDE)  as DATE) as RTN_DAY_DTE
, tkt.OPN_IORG_ID
, tkt.RENT_CNTRCT_NBR
, tkt.RESV_ORIG_CDE
, tkt.PCKUP_DTE
, TKT.CNTRCT_CRED_EMP_ID
, TKT.CREATED_BY as CNTRCT_CRTE_EMP_ID
-- changes start January 2015 
, TKT.ORIG_BRAND_ID
-- changes end January 2015	
, COALESCE(RTM.NEW_RENT_TYP_CDE,TKT.RENT_TYP_CDE) AS RENT_TYP_CDE
, tkt.LPARM_RATE_PLAN_NBR AS RATE_PLAN_NBR
, amr.CUST_ACCT_GID AS RATE_SRCE_ACCT_GID
, cst.account_nbr AS RATE_SRCE_CUST_NBR
-- Modified fot CUST_MAST decommissioning DW CQ RENTL00232659 
, tkt.CUST_RATE_SRCE_NBR AS RATE_SRCE_CUST_SEQ_NBR
, ams.CUST_ACCT_GID AS SHOP_ACCT_GID
, shp.CUST_SEQ_NBR AS SHOP_CUST_SEQ_NBR
, shp.CUST_NBR AS SHOP_CUST_NBR
, tkt.CAR_CLS_ID AS CAR_CLS_RSVD
, tktrte.CAR_CLS_CDE AS CAR_CLS_CHRGD
, SYSLIB.ERAC_GMT_TO_LCL(cls.CREATE_TIMESTAMP,ORG.TIM_ZON_CDE) as CLS_TKT_TSP
, cls.RENT_CNTRCT_TOT_DAY_CHRG_QTY
, tkt.BTI_NCLD_FLG
-- Added for  DW CQ RENTL00232659 
, tkt.gui_resv_nbr
, tkt.orig_rwrt_rent_cntrct_nbr
, tkt.arms_auth_ind
, tkt.lgcy_quote_rate_amt
FROM ECARS2.RENT_CNTRCT tkt
INNER JOIN
ECARS2_RPT_WT.CLOS_TKT_TOT_WT CLS
ON TKT.RENT_CNTRCT_NBR = CLS.RENT_CNTRCT_NBR
AND TKT.CURR_VRSN_IND = 'Y'
AND TKT.ROW_STAT_CDE = 'A'
AND (TKT.RECORD_STATUS IS NULL OR TKT.RECORD_STATUS = 'A')
AND TKT.RENT_CNTRCT_STAT_CDE = 3
LEFT JOIN
ECARS2.RENT_CNTRCT_RATE tktrte
ON TKT.RENT_CNTRCT_NBR = tktrte.RENT_CNTRCT_NBR
AND tktrte.CURR_VRSN_IND = 'Y'
AND tktrte.ROW_STAT_CDE = 'A'
AND tktrte.RECORD_STATUS = 'A'
AND tkt.RTN_DTE = tktrte.RENT_CNTRCT_RATE_END_DTE
AND tktrte.RENT_CNTRCT_RATE_STRT_DTE <> tktrte.RENT_CNTRCT_RATE_END_DTE
AND tktrte.RENT_RATE_TYP_CDE = 1
--Rate Src Nbr
-- Modified fot CUST_MAST decommissioning DW CQ RENTL00232659 
LEFT JOIN INTGRT_RPT.DIM_ACCOUNT  cst
ON tkt.CUST_RATE_SRCE_NBR = cst.ERAC_CUST_SEQ_NBR
 	--Acct Maint
-- Modified fot CUST_MAST decommissioning DW CQ RENTL00232659 
LEFT JOIN CAM.CUST_ACCT amr
ON cst.account_nbr = amr.CUST_NBR
AND amr.ROW_STAT_CDE = 'A'
 --Shop Nbr
LEFT JOIN ECARS2.RENT_CUST shp
ON tkt.RENT_CNTRCT_NBR = shp.RENT_CNTRCT_NBR
AND shp.CURR_VRSN_IND = 'Y'
AND shp.ROW_STAT_CDE = 'A'
AND (shp.record_status <> 'D' OR shp.RECORD_STATUS IS NULL)
AND shp.ROLE_TYP_CDE = 2
  	--Acct Maint
LEFT JOIN CAM.CUST_ACCT ams
ON shp.CUST_NBR = ams.CUST_NBR
AND ams.ROW_STAT_CDE = 'A'
INNER JOIN
ECARS2.OFC_DIR_BR org
ON TKT.OPN_IORG_ID = ORG.IORG_ID
AND ORG.CURR_VRSN_IND = 'Y'
AND ORG.ROW_STAT_CDE = 'A'
AND ORG.RECORD_STATUS = 'A'
AND UPPER(TRIM(ORG.CNTRY_ISO_CDE)) IN (${CNTRY_CDE})
LEFT JOIN
REFERENCE.HC_ACCT_TYP RTM
ON TKT.CUST_RATE_SRCE_NBR = RTM.CUST_SEQ_NBR AND
ORG.CNTRY_ISO_CDE = RTM.CNTRY_ISO_CDE
WHERE
SYSLIB.ERAC_GMT_TO_LCL(cls.CREATE_TIMESTAMP,ORG.TIM_ZON_CDE) >=
CAST((cls.dte_START_CST_DATE - 6 ) AS TIMESTAMP FORMAT 'yyyy-MM-dd:HH:mm:ss')
AND SYSLIB.ERAC_GMT_TO_LCL(cls.CREATE_TIMESTAMP,ORG.TIM_ZON_CDE) <
cls.dte_END_CST_DATE + ':00:00:00'
) WITH DATA
PRIMARY INDEX (OPN_IORG_ID, rent_cntrct_nbr)
ON COMMIT PRESERVE ROWS;
""").show(truncate=False) 

# COMMAND ----------
if xSqlState.ERROR_CODE != 0:
  
  dbutils.notebook.exit(xSqlState.ERROR_CODE)  

# FIXME databricks.migration.unsupported.feature Teradata 'VOLATILE' Table type

# COMMAND ----------
spark.sql("""
CREATE TABLE TKT_TEMP2 AS
(
SELECT
TKT.RTN_MTH_KEY_ID
, TKT.OPN_IORG_ID
, TKT.RENT_CNTRCT_NBR
--	changes start January 2015	
, TKT.ORIG_BRAND_ID
--	changes end January 2015	
, TKT.CNTRCT_CRED_EMP_ID
, TKT.CNTRCT_CRTE_EMP_ID
, TKT.RENT_TYP_CDE
, TKT.PCKUP_DTE
, TKT.RTN_DAY_DTE
, TKT.CLS_TKT_TSP
, TKT.RATE_PLAN_NBR
, TKT.RATE_SRCE_ACCT_GID
, TKT.RATE_SRCE_CUST_NBR
, TKT.RATE_SRCE_CUST_SEQ_NBR
, TKT.SHOP_ACCT_GID
, TKT.SHOP_CUST_SEQ_NBR
, TKT.SHOP_CUST_NBR
, TKT.RENT_CNTRCT_TOT_DAY_CHRG_QTY
, TKT.GUI_RESV_NBR
, TKT.ORIG_RWRT_RENT_CNTRCT_NBR
, SUM(
CASE WHEN TRIM(DTL.RENT_CNTRCT_CHRG_TYP_CDE)  = '00001' THEN DTL.CLOS_TKT_TOT_DTL_CHRG_AMT
ELSE 0.00
END
) AS TD_AMT
, MIN(
CASE                                                
--Eligible DW Days
WHEN dtl.CLOS_TKT_TOT_DTL_NCLV_IND = 1 AND TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00006' 
THEN 0
ELSE TKT.RENT_CNTRCT_TOT_DAY_CHRG_QTY
END
) AS  ELGBL_DW_DAYS_QTY
, SUM(
CASE                                                           
--DW 
WHEN TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00006' THEN dtl.CLOS_TKT_TOT_DTL_CHRG_AMT
ELSE 0.00 
END
) as DW_AMT
-- changes start May 2014
-- remove excess from calculations
, MIN(
CASE                           
--Eligible PAI  00209 = PAI/PEC AND 00007 = PAI 
WHEN dtl.CLOS_TKT_TOT_DTL_NCLV_IND = 1 
AND 
(TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00209' 
OR TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00007' 
OR TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00206'
) 
THEN 0
ELSE TKT.RENT_CNTRCT_TOT_DAY_CHRG_QTY
END
) AS  elgbl_pai_days_qty
-- remove excess from calculations
, SUM(
CASE                         
--PAI  00209 = PAI/PEC AND 00007 = PAI  
WHEN TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00209' 
OR TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00007' 
OR TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00206'
THEN dtl.CLOS_TKT_TOT_DTL_CHRG_AMT
ELSE 0.00 
END
) AS PAI_AMT
-- new 
,MIN(
CASE                                                
--Eligible EXCESS Days
WHEN dtl.CLOS_TKT_TOT_DTL_NCLV_IND = 1 AND TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00207' 
THEN 0
ELSE TKT.RENT_CNTRCT_TOT_DAY_CHRG_QTY
END
) AS  elgbl_excess_days_qty
,SUM(
CASE                                                           
--EXCESS 
WHEN TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00207' THEN dtl.CLOS_TKT_TOT_DTL_CHRG_AMT
ELSE 0.00 
END
) AS EXCESS_AMT
-- end new
-- changes end  May 2014	
, MIN(
CASE                                                           
--Eligible SLP Days
WHEN dtl.CLOS_TKT_TOT_DTL_NCLV_IND = 1 AND TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00032' THEN 0
WHEN tkt.BTI_NCLD_FLG = 1  THEN 0  
-- bti added June, 2015 fo for  DW CQ RENTL00232659   
ELSE TKT.RENT_CNTRCT_TOT_DAY_CHRG_QTY
END
) AS  elgbl_slp_days_qty
, SUM(
CASE                                                           
--SLP 
WHEN TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00032' THEN dtl.CLOS_TKT_TOT_DTL_CHRG_AMT
ELSE 0.00
END
) as SLP_AMT
, MIN(
CASE                           
--Eligible GPS  00247 = GPS UNIT- CAR   00254 = GPS UNIT  00224= SAT NAV Days
WHEN dtl.CLOS_TKT_TOT_DTL_NCLV_IND = 1 
AND
(TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00247' 
OR TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00254' 
OR TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00224') 		
THEN 0
ELSE TKT.RENT_CNTRCT_TOT_DAY_CHRG_QTY
END
) AS  elgbl_gps_days_qty
, SUM(
CASE                 
--GPS  00247 = GPS UNIT- CAR   00254 = GPS UNIT  00224= SAT NAV
WHEN TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00247' 
OR TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00254' 
OR TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00224' THEN dtl.CLOS_TKT_TOT_DTL_CHRG_AMT
ELSE 0.00
END
) as GPS_AMT
-- changes start May 2014
-- modifications start
, MIN(
CASE                           
--Eligible RAP  00279 = RAP AND 05033 = TKR 
WHEN dtl.CLOS_TKT_TOT_DTL_NCLV_IND = 1 
AND 
(TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00279' 
OR TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '05033' 
) 
THEN 0
ELSE TKT.RENT_CNTRCT_TOT_DAY_CHRG_QTY
END
) AS  elgbl_rap_days_qty
, SUM(
CASE                         
--RAP  00209 = RAP AND 05033 = TKR  
WHEN TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00279' 
OR TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '05033' 
THEN dtl.CLOS_TKT_TOT_DTL_CHRG_AMT
ELSE 0.00 
END
) AS RAP_AMT
-- modifications end
-- changes end  May 2014	
, MIN(
CASE      
--Eligible Pre-Paid Fuel Days
WHEN dtl.CLOS_TKT_TOT_DTL_NCLV_IND = 1 
AND 
(TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00035' 
OR UPPER(TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE)) = 'F')
THEN 0
ELSE TKT.RENT_CNTRCT_TOT_DAY_CHRG_QTY
END
) AS  elgbl_pre_paid_fuel_days_qty
, SUM(
CASE                                                
-- Pre-Paid Fuel 
WHEN TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00035' THEN dtl.CLOS_TKT_TOT_DTL_CHRG_AMT
WHEN UPPER(TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE)) = 'F' THEN dtl.CLOS_TKT_TOT_DTL_CHRG_AMT
ELSE 0.00
END
) as PRE_PAID_FUEL_AMT
, MIN(
CASE                                                       
--Eligible BlueTooth Days
WHEN dtl.CLOS_TKT_TOT_DTL_NCLV_IND = 1  AND (TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00248' )
THEN 0
ELSE TKT.RENT_CNTRCT_TOT_DAY_CHRG_QTY
END
) AS  elgbl_bluetooth_days_qty
, SUM(
CASE                                                
-- BlueTooth  
WHEN TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00248' THEN dtl.CLOS_TKT_TOT_DTL_CHRG_AMT
ELSE 0.00
END
) AS BLUETOOTH_AMT
, MIN(
CASE                                                       
--Eligible WiFi Days
WHEN dtl.CLOS_TKT_TOT_DTL_NCLV_IND = 1  AND (TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00287' )
THEN 0
ELSE TKT.RENT_CNTRCT_TOT_DAY_CHRG_QTY
END
) AS  elgbl_wifi_days_qty
, SUM(
CASE                                                
-- WiFi 
WHEN TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00287' THEN dtl.CLOS_TKT_TOT_DTL_CHRG_AMT
ELSE 0.00
END
) AS WIFI_AMT
, MIN(
CASE                           
--Eligible 00282 - SATELLITE RADIO  - CAR, 00283 - SATELLITE RADIO - TRUCK  
WHEN dtl.CLOS_TKT_TOT_DTL_NCLV_IND = 1 
AND 
(
TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00282' 
OR 
TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00283' 
) 
THEN 0
ELSE TKT.RENT_CNTRCT_TOT_DAY_CHRG_QTY
END
) AS  elgbl_satellite_radio_days_qty
, SUM(
CASE                          
-- 00282 - SATELLITE RADIO  - CAR, 00283 - SATELLITE RADIO - TRUCK	
WHEN TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00282' 
OR TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00283' 
THEN dtl.CLOS_TKT_TOT_DTL_CHRG_AMT
ELSE 0.00 
END
) AS SATELLITE_RADIO_AMT
, MIN(
CASE                                                       
--Eligible COE Days
WHEN dtl.CLOS_TKT_TOT_DTL_NCLV_IND = 1  AND (TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '05234' )
THEN 0
ELSE TKT.RENT_CNTRCT_TOT_DAY_CHRG_QTY
END
) AS  elgbl_coe_days_qty
, SUM(
CASE                                                
-- COE 
WHEN TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '05234' THEN dtl.CLOS_TKT_TOT_DTL_CHRG_AMT
ELSE 0.00
END
) AS COE_AMT
,SUM(
CASE            
WHEN TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00244'                 
OR TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00260'           
OR TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00242'            
OR TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00245'            
OR TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00258'            
OR TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00246'            
OR TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00262'            
OR TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00243'            
OR TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00259'            
OR TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00281'            
OR TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00284'            
OR TRIM(dtl.RENT_CNTRCT_CHRG_TYP_CDE) = '00285'            
THEN dtl.CLOS_TKT_TOT_DTL_CHRG_AMT                
ELSE 0.00                 
END
)AS ADDL_TRUCK_PRODUCTS_AMT
FROM TKT_TEMP1 tkt
INNER JOIN
ECARS2.CLOS_TKT_TOT_DTL dtl
ON TKT.RENT_CNTRCT_NBR = dtl.RENT_CNTRCT_NBR
WHERE dtl.CURR_VRSN_IND = 'Y'
AND dtl.ROW_STAT_CDE = 'A'
AND dtl.RECORD_STATUS = 'A'
AND UPPER(TRIM(dtl.CLOS_TKT_TOT_DTL_ACTV_CDE)) = 'AS'
AND dtl.PRTY_ACCT_ID IS NULL
AND (dtl.VEH_ID = 0
OR
dtl.VEH_ID IS NULL)
GROUP BY
TKT.RTN_MTH_KEY_ID
, TKT.OPN_IORG_ID
, TKT.RENT_CNTRCT_NBR
, TKT.CNTRCT_CRED_EMP_ID
, TKT.CNTRCT_CRTE_EMP_ID
--	changes start January 2015	
, TKT.ORIG_BRAND_ID
--	changes end January 2015	
, TKT.RENT_TYP_CDE
, TKT.PCKUP_DTE
, TKT.RTN_DAY_DTE
, TKT.CLS_TKT_TSP
, TKT.RATE_PLAN_NBR
, TKT.RATE_SRCE_ACCT_GID
, TKT.RATE_SRCE_CUST_NBR
, TKT.RATE_SRCE_CUST_SEQ_NBR
, TKT.SHOP_ACCT_GID
, TKT.SHOP_CUST_SEQ_NBR
, TKT.SHOP_CUST_NBR
, TKT.RENT_CNTRCT_TOT_DAY_CHRG_QTY
, TKT.GUI_RESV_NBR
, TKT.ORIG_RWRT_RENT_CNTRCT_NBR
) WITH DATA
PRIMARY INDEX (OPN_IORG_ID, rent_cntrct_nbr)
ON COMMIT PRESERVE ROWS;
""").show(truncate=False) 

# COMMAND ----------
if xSqlState.ERROR_CODE != 0:
  
  dbutils.notebook.exit(xSqlState.ERROR_CODE)  

# GET SELLUP 
 /*
Add new start May 2014
*/
 # calculate reservation rate removing taxes from tax inclusive reservations from Odyssey	
 # FIXME databricks.migration.unsupported.feature Teradata 'VOLATILE' Table type

# COMMAND ----------
spark.sql("""
CREATE TABLE Ody_res_temp1 AS
(
SELECT
rent_cntrct_nbr,
SUM(PLF_percent) AS PLF_percent,
SUM(PLF_per_rental) AS PLF_per_rental,
SUM(PLF_per_day) AS PLF_per_day,
SUM(VAT_percent) AS VAT_percent
FROM
(
SELECT dtl.rent_cntrct_nbr AS rent_cntrct_nbr,
dtl.clos_tkt_tot_dtl_chrg_rate_amt AS charged_vat,
ROW_NUMBER() OVER(PARTITION BY dtl.rent_cntrct_nbr,dtl.rent_cntrct_chrg_typ_cde ORDER BY clos_tkt_tot_dtl_seq_nbr ) AS rownum
-- Premium Location Fee Charged based on a Percentage
,	MAX (
CASE WHEN dtl.rent_cntrct_chrg_typ_cde = 'ARPT'  AND dtl.perd_Typ_cde =6 THEN dtl.clos_tkt_tot_dtl_chrg_rate_amt ELSE 0.00 END
)/100 AS PLF_percent
-- Premium Location Fee Charged based per Rental Fee
,	MAX (
CASE WHEN dtl.rent_cntrct_chrg_typ_cde = 'ARPT'  AND dtl.perd_Typ_cde =4 THEN dtl.clos_tkt_tot_dtl_chrg_rate_amt ELSE 0.00 END
) AS PLF_per_rental
-- Premium Location Fee Charged based on a per Day
,	MAX (
CASE WHEN dtl.rent_cntrct_chrg_typ_cde = 'ARPT'  AND dtl.perd_Typ_cde IN(1,2,3,7) THEN dtl.clos_tkt_tot_dtl_chrg_rate_amt ELSE 0.00 END
) AS PLF_per_day
-- Value Added Tax based on a Percentage
,	MAX(
CASE WHEN dtl.rent_cntrct_chrg_typ_cde IN ('SVAT','VAT','IVA') THEN dtl.clos_tkt_tot_dtl_chrg_rate_amt ELSE 0.00 END
)/100 AS VAT_percent
FROM  ECARS2.CLOS_TKT_TOT_DTL dtl,
TKT_TEMP1 tkts,
ECARS2.RENT_CNTRCT_CHRG rcc
WHERE  tkts.rent_cntrct_nbr = dtl.rent_cntrct_nbr
AND 	dtl.rent_cntrct_chrg_typ_cde IN ('VAT','SVAT','ARPT','IVA')
AND  	dtl.CLOS_TKT_TOT_DTL_ACTV_CDE = 'AS'
AND	dtl.PRTY_ACCT_ID IS NULL
AND	(dtl.VEH_ID = 0 OR dtl.VEH_ID IS NULL)
AND	dtl.CURR_VRSN_IND = 'Y'
AND	dtl.ROW_STAT_CDE = 'A'
AND	dtl.RECORD_STATUS = 'A'
AND	rcc.rent_cntrct_nbr = tkts.RENT_CNTRCT_NBR
AND 	rcc.rent_cntrct_chrg_typ_cde = dtl.rent_cntrct_chrg_typ_cde
AND 	(rcc.RECORD_STATUS IS NULL OR  rcc.RECORD_STATUS <> 'D' )
AND    rcc.ROW_STAT_CDE  = 'A'
AND    rcc.CURR_VRSN_IND = 'Y'
AND 	rcc.rent_cntrct_chrg_ncld_ind =1
-- tax included
GROUP BY 1,2,dtl.rent_cntrct_chrg_typ_cde,clos_tkt_tot_dtl_seq_nbr
QUALIFY rownum=1
) rates
GROUP BY 1
) WITH DATA
PRIMARY INDEX ( rent_cntrct_nbr)
ON COMMIT PRESERVE ROWS;
""").show(truncate=False) 

# COMMAND ----------
if xSqlState.ERROR_CODE != 0:
  
  dbutils.notebook.exit(xSqlState.ERROR_CODE)  

# FIXME databricks.migration.unsupported.feature Teradata 'VOLATILE' Table type

# COMMAND ----------
spark.sql("""
CREATE TABLE Ody_res_rate AS
(
SELECT	tkt.rent_cntrct_nbr  ,
tktDtl.CLOS_TKT_TOT_DTL_CHRG_AMT
, RENT_CNTRCT_TOT_DAY_CHRG_QTY AS days
, tktDtl.PERD_TYP_CDE AS  PERD_TYP_CDE
, tktDtl.CLOS_TKT_TOT_DTL_QTY AS number_of_rate_type
,
CASE  
WHEN (nrate.RTD_TYPE <> 'TER' OR nrate.RTD_TYPE IS NULL) AND tktDtl.PERD_TYP_CDE = 1 AND resRt.VEH_DAY_RATE_AMT > 0.000 
THEN  resRt.VEH_DAY_RATE_AMT
WHEN  tktDtl.PERD_TYP_CDE = 2 AND resRt.VEH_WEEK_RATE_AMT > 0.000 
THEN resRt.VEH_WEEK_RATE_AMT
WHEN tktDtl.PERD_TYP_CDE = 3 AND resRt.VEH_MTH_RATE_AMT > 0.000 
THEN resRt.VEH_MTH_RATE_AMT
WHEN tktDtl.PERD_TYP_CDE = 7 AND resRt.VEH_HR_RATE_AMT > 0.000 
THEN resRt.VEH_HR_RATE_AMT
END
AS Res_rate
,	CAST (
CASE	
--  Daily , Weekly, Monthly and Hourly Rates charged a Percentage
WHEN	PLF_percent > 0 AND	tktDtl.PERD_TYP_CDE IN (1,2,3,7) 
THEN Res_rate / (ctdr.PLF_percent + ctdr.VAT_percent + (ctdr.VAT_percent * ctdr.PLF_percent) + 1) 
--  Daily Rates charged a Per Retnal Fee
WHEN PLF_percent = 0  AND	tktDtl.PERD_TYP_CDE =1 
THEN (Res_rate - (ctdr.PLF_per_rental / days) -  (ctdr.PLF_per_rental / days) * ctdr.VAT_percent) / (1+ctdr.VAT_percent)
--  Weekly Rates charged a Per Retnal Fee
WHEN PLF_percent = 0 AND	tktDtl.PERD_TYP_CDE =2 
THEN (Res_rate - (((ctdr.PLF_per_rental / days)* tktDtl.CLOS_TKT_TOT_DTL_DAY_CHRG_QTY)/ tktDtl.CLOS_TKT_TOT_DTL_QTY) -   (((ctdr.PLF_per_rental / days)* tktDtl.CLOS_TKT_TOT_DTL_DAY_CHRG_QTY)
/ tktDtl.CLOS_TKT_TOT_DTL_QTY) * ctdr.VAT_percent) / (1+ctdr.VAT_percent)
--  Monthly Rates charged a Per Retnal Fee and total duration less then 30 days	
WHEN PLF_percent = 0 	AND	tktDtl.PERD_TYP_CDE =3  AND	days < 30
THEN (Res_rate - (((ctdr.PLF_per_rental / days)* tktDtl.CLOS_TKT_TOT_DTL_DAY_CHRG_QTY)/ tktDtl.CLOS_TKT_TOT_DTL_QTY) -   (((ctdr.PLF_per_rental / days)* tktDtl.CLOS_TKT_TOT_DTL_DAY_CHRG_QTY)
/ tktDtl.CLOS_TKT_TOT_DTL_QTY) * ctdr.VAT_percent) / (1+ctdr.VAT_percent)
--  Monthly Rates charged a Per Retnal Fee and total duration greater than or qeual to 30 days	
WHEN PLF_percent = 0 AND	tktDtl.PERD_TYP_CDE =3 AND	days >= 30 
THEN (Res_rate - (((ctdr.PLF_per_rental / days)* 30)/ tktDtl.CLOS_TKT_TOT_DTL_QTY) -   (((ctdr.PLF_per_rental / days)* 30)/ tktDtl.CLOS_TKT_TOT_DTL_QTY) * ctdr.VAT_percent) 
/ (1+ctdr.VAT_percent)
--  Weekly Rates charged a Per Retnal Fee on the ticket		
WHEN PLF_percent = 0 	AND	tktDtl.PERD_TYP_CDE =7 
THEN (Res_rate  / (1+ctdr.VAT_percent))
END
AS DECIMAL (20,10) )  AS calc_Res_Rate
FROM
TKT_TEMP1 tkt
INNER JOIN
ECARS2.CLOS_TKT_TOT_DTL tktDtl
ON  tkt.RENT_CNTRCT_NBR = tktDtl.RENT_CNTRCT_NBR AND
tktDtl.CLOS_TKT_TOT_DTL_ACTV_CDE = 'AS' AND
tktDtl.PRTY_ACCT_ID IS NULL AND
(tktDtl.VEH_ID = 0 OR tktDtl.VEH_ID IS NULL) AND
tktDtl.RENT_CNTRCT_CHRG_TYP_CDE = '00001' AND
tktDtl.CURR_VRSN_IND = 'Y' AND
tktDtl.ROW_STAT_CDE = 'A' AND
tktDtl.RECORD_STATUS = 'A'
JOIN
ECARS2.RENT_CNTRCT_ASSOC crs
ON tktDtl.RENT_CNTRCT_NBR = crs.PARNT_RENT_CNTRCT_NBR
AND crs.CURR_VRSN_IND = 'Y'
AND crs.ROW_STAT_CDE = 'A'
JOIN
ECARS2.RENT_CNTRCT res
ON crs.CHILD_RENT_CNTRCT_NBR = res.RENT_CNTRCT_NBR
AND res.CURR_VRSN_IND = 'Y'
AND res.ROW_STAT_CDE = 'A'
AND (res.RECORD_STATUS IS NULL OR res.RECORD_STATUS <> 'D')
AND res.SRCE_CHNL_CDE = 1
-- Odyssey Reservation
JOIN
ECARS2.RENT_CNTRCT_RATE resRt
ON crs.CHILD_RENT_CNTRCT_NBR = resRt.RENT_CNTRCT_NBR
AND resRt.CURR_VRSN_IND = 'Y'
AND resRt.ROW_STAT_CDE = 'A'
AND resRt.RECORD_STATUS = 'A'
AND tktDtl.RENT_FEE_CAT_CDE  = resRt.RENT_RATE_TYP_CDE
LEFT JOIN ECARS2.RATE_DETAILS NRATE
ON NRATE.RH_RTH_UNIQ_ID = RESRT.RATE_ID AND
NRATE.RTD_ID =RESRT.RATE_DTL_ID AND
NRATE.ROW_STAT_CDE = 'A' AND
NRATE.CURR_VRSN_IND = 'Y' AND
(NRATE.RECORD_STATUS IS NULL OR NRATE.RECORD_STATUS <> 'D' )
LEFT JOIN Ody_res_temp1 ctdr
ON tkt.RENT_CNTRCT_NBR = ctdr.RENT_CNTRCT_NBR
) WITH DATA
PRIMARY INDEX (rent_cntrct_nbr,PERD_TYP_CDE)
ON COMMIT PRESERVE ROWS;
""").show(truncate=False) 

# COMMAND ----------
if xSqlState.ERROR_CODE != 0:
  
  dbutils.notebook.exit(xSqlState.ERROR_CODE)  

# FIXME databricks.migration.unsupported.feature Teradata 'VOLATILE' Table type

# COMMAND ----------
spark.sql("""
CREATE TABLE Ody_Tour_Res_Charges AS
(
SELECT rent_cntrct_nbr,
CLOS_TKT_TOT_DTL_CHRG_AMT,
CAST ( number_of_rate_type * calc_Res_Rate AS DECIMAL (10,2)) AS myOdyResChrg
FROM Ody_res_rate
WHERE Calc_res_rate IS NOT NULL AND Calc_res_rate >0
) WITH DATA
PRIMARY INDEX ( rent_cntrct_nbr)
ON COMMIT PRESERVE ROWS;
""").show(truncate=False) 

# COMMAND ----------
if xSqlState.ERROR_CODE != 0:
  
  dbutils.notebook.exit(xSqlState.ERROR_CODE)  

#****************************************************************************************
 /*
Add new  end   May 2014
*/
 # FIXME databricks.migration.unsupported.feature Teradata 'VOLATILE' Table type

# COMMAND ----------
spark.sql("""
CREATE TABLE SELLUP_TMP AS
(
Select
OPN_IORG_ID
,RENT_CNTRCT_NBR
,SUM(SELLUP) as SELLUP
FROM
(
Select
tkt.OPN_IORG_ID
,tkt.RENT_CNTRCT_NBR
,tktDtl.CLOS_TKT_TOT_DTL_CHRG_AMT
,
CASE
/*
Add new start May 2014
*/
WHEN Ody_Res_Charge.rent_cntrct_nbr is not null then Ody_Res_Charge.myOdyResChrg
/*
Add new end
May 2014
*/
WHEN (nrate.RTD_TYPE <> 'TER' OR nrate.RTD_TYPE IS NULL) AND tktDtl.PERD_TYP_CDE == 1
AND resRt.VEH_DAY_RATE_AMT > 0.000
THEN tktDtl.CLOS_TKT_TOT_DTL_QTY * resRt.VEH_DAY_RATE_AMT
WHEN  tktDtl.PERD_TYP_CDE == 2 AND resRt.VEH_WEEK_RATE_AMT > 0.000
THEN tktDtl.CLOS_TKT_TOT_DTL_QTY * resRt.VEH_WEEK_RATE_AMT
WHEN tktDtl.PERD_TYP_CDE == 3 AND resRt.VEH_MTH_RATE_AMT > 0.000
THEN tktDtl.CLOS_TKT_TOT_DTL_QTY * resRt.VEH_MTH_RATE_AMT
WHEN tktDtl.PERD_TYP_CDE == 7 AND resRt.VEH_HR_RATE_AMT > 0.000
THEN tktDtl.CLOS_TKT_TOT_DTL_QTY * resRt.VEH_HR_RATE_AMT
WHEN tktDtl.PERD_TYP_CDE == 1 AND nrate.RTD_TYPE = 'TER'  AND tr.DLY_RENT_RATE_AMT> 0.000
THEN tktDtl.CLOS_TKT_TOT_DTL_QTY * tr.DLY_RENT_RATE_AMT
END AS myResChrg
,
CASE
WHEN res.arms_auth_ind = 1 AND resRt.VEH_DAY_RATE_AMT = 0.000 
AND res.lgcy_quote_rate_amt > 0.000 
AND  ( nrate.RTD_TYPE <> 'TER' OR nrate.RTD_TYPE IS NULL) AND tktDtl.PERD_TYP_CDE = 1 
THEN  tktDtl.CLOS_TKT_TOT_DTL_QTY * res.lgcy_quote_rate_amt
ELSE 0
END
AS lgcyResChrg
,
CASE
WHEN res.arms_auth_ind = 1 AND res.lgcy_quote_rate_amt > 0.000 AND resRt.VEH_DAY_RATE_AMT = 0.000 
AND lgcyResChrg   > 0 and tktDtl.CLOS_TKT_TOT_DTL_CHRG_AMT - lgcyResChrg > 0
THEN  tktDtl.CLOS_TKT_TOT_DTL_CHRG_AMT - lgcyResChrg
WHEN myResChrg is NULL THEN 0.000
WHEN '${CHCK_CAR_CLSS}' = 'Y' AND tkt.CAR_CLS_RSVD = tkt.CAR_CLS_CHRGD 
AND   tkt.RESV_ORIG_CDE = 1 THEN 0.000
WHEN '${CHCK_CAR_CLSS}' = 'Y' AND resRt.CAR_CLS_CDE= tkt.CAR_CLS_CHRGD 
AND tkt.RESV_ORIG_CDE = 1 THEN 0.000
WHEN '${CHCK_RES_TO_TKT_TM}' = 'Y' 
AND tkt.RESV_ORIG_CDE <> 1 
AND 
(
EXTRACT 
(
DAY FROM 
(
tkt.PCKUP_DTE - res.CREATE_TIMESTAMP
DAY(4) TO MINUTE
)
) 
) = 0
AND 
(
EXTRACT 
(
HOUR FROM 
(
tkt.PCKUP_DTE - res.CREATE_TIMESTAMP
DAY(4) TO MINUTE
)
) 
) < 1
THEN 0.000
WHEN tktDtl.CLOS_TKT_TOT_DTL_CHRG_AMT - myResChrg > 0.000 THEN tktDtl.CLOS_TKT_TOT_DTL_CHRG_AMT - myResChrg
ELSE 0.000
END
as SELLUP
From
TKT_TEMP1 tkt
INNER JOIN
ECARS2.CLOS_TKT_TOT_DTL tktDtl
ON  tkt.RENT_CNTRCT_NBR = tktDtl.RENT_CNTRCT_NBR And
tktDtl.CLOS_TKT_TOT_DTL_ACTV_CDE = 'AS' and
tktDtl.PRTY_ACCT_ID IS NULL and
(tktDtl.VEH_ID = 0 OR tktDtl.VEH_ID IS NULL) And
tktDtl.RENT_CNTRCT_CHRG_TYP_CDE = '00001' and
tktDtl.CURR_VRSN_IND = 'Y' and
tktDtl.ROW_STAT_CDE = 'A' and
tktDtl.RECORD_STATUS = 'A'
JOIN
ECARS2.RENT_CNTRCT_ASSOC crs
ON tktDtl.RENT_CNTRCT_NBR = crs.PARNT_RENT_CNTRCT_NBR
and crs.CURR_VRSN_IND = 'Y'
and crs.ROW_STAT_CDE = 'A'
JOIN
ECARS2.RENT_CNTRCT res
ON crs.CHILD_RENT_CNTRCT_NBR = res.RENT_CNTRCT_NBR
AND res.CURR_VRSN_IND = 'Y'
AND res.ROW_STAT_CDE = 'A'
AND (res.RECORD_STATUS IS NULL OR res.RECORD_STATUS <> 'D')
JOIN
ECARS2.RENT_CNTRCT_RATE resRt
ON crs.CHILD_RENT_CNTRCT_NBR = resRt.RENT_CNTRCT_NBR
and resRt.CURR_VRSN_IND = 'Y'
and resRt.ROW_STAT_CDE = 'A'
and resRt.RECORD_STATUS = 'A'
and tktDtl.RENT_FEE_CAT_CDE  = resRt.RENT_RATE_TYP_CDE
LEFT JOIN ECARS2.RATE_DETAILS NRATE
ON NRATE.RH_RTH_UNIQ_ID = RESRT.RATE_ID AND
NRATE.RTD_ID =RESRT.RATE_DTL_ID AND
NRATE.ROW_STAT_CDE = 'A' AND
NRATE.CURR_VRSN_IND = 'Y' AND
(NRATE.RECORD_STATUS IS NULL OR NRATE.RECORD_STATUS <> 'D' )
LEFT JOIN ECARS2.RENT_TIER_RATE TR
ON RESRT.RATE_DTL_ID = TR.RTD_ID AND
TR.CURR_VRSN_IND = 'Y' AND
TR.ROW_STAT_CDE = 'A' AND
(TR.RECORD_STATUS IS NULL OR TR.RECORD_STATUS <> 'D' ) AND
( ( TKT.RENT_CNTRCT_TOT_DAY_CHRG_QTY >= TR.MIN_RENT_RATE_DURTN_QTY AND
TKT.RENT_CNTRCT_TOT_DAY_CHRG_QTY <= TR.MAX_RENT_RATE_DURTN_QTY
) OR
( TKT.RENT_CNTRCT_TOT_DAY_CHRG_QTY >= TR.MIN_RENT_RATE_DURTN_QTY AND
TR.MAX_RENT_RATE_DURTN_QTY IS NULL
)
)
/*
Add new start May 2014
*/
Left outer join Ody_Tour_Res_Charges Ody_Res_Charge
On tkt.rent_cntrct_nbr = Ody_Res_Charge.rent_cntrct_nbr
And tktDtl.CLOS_TKT_TOT_DTL_CHRG_AMT = Ody_Res_Charge.CLOS_TKT_TOT_DTL_CHRG_AMT
/*
Add new end May 2014
*/
) t
GROUP BY 1,2
) WITH DATA
PRIMARY INDEX (OPN_IORG_ID, rent_cntrct_nbr)
ON COMMIT PRESERVE ROWS;
""").show(truncate=False) 

# COMMAND ----------
if xSqlState.ERROR_CODE != 0:
  
  dbutils.notebook.exit(xSqlState.ERROR_CODE)  


# COMMAND ----------
spark.sql("""
DELETE FROM ECARS2_RPT_TB.HOME_CITY_EMPL_CNTRCT_SALES_N;
""").show(truncate=False) 

# COMMAND ----------
if xSqlState.ERROR_CODE != 0:
  
  dbutils.notebook.exit(xSqlState.ERROR_CODE)  

#  STEP 3 LOAD TODAYS DATA INTO TABLE N WHICH IS NOT USED BY THE VIEW 

# COMMAND ----------
spark.sql("""
INSERT INTO ECARS2_RPT_TB.HOME_CITY_EMPL_CNTRCT_SALES_N
(   RTN_MTH_KEY_ID
, OPN_IORG_ID
--	new changes begin January 2015		
, ORIG_BRAND_ID
--	new changes end January 2015			
, RENT_CNTRCT_NBR
, RATE_PLAN_NBR
, RATE_SRCE_ACCT_GID
, RATE_SRCE_CUST_NBR
, RATE_SRCE_CUST_SEQ_NBR
, SHOP_ACCT_GID
, SHOP_CUST_SEQ_NBR
, SHOP_CUST_NBR
, CNTRCT_CRED_EMP_ID
, CNTRCT_CRTE_EMP_ID
, RENT_TYP_CDE
, PCKUP_DTE
, RTN_DAY_DTE
, TKT_CLOS_TSP
, GUI_RESV_NBR
, ORIG_RWRT_RENT_CNTRCT_NBR
, RENT_DAYS_QTY
, TD_AMT
, ELGBL_DW_DAYS_QTY
, DW_AMT
, elgbl_pai_days_qty
, PAI_AMT
, elgbl_excess_days_qty
, EXCESS_AMT
, elgbl_slp_days_qty
, SLP_AMT
, elgbl_gps_days_qty
, GPS_AMT
, elgbl_rap_days_qty
, RAP_AMT
, elgbl_pre_paid_fuel_days_qty
, PRE_PAID_FUEL_AMT
, elgbl_bluetooth_days_qty
, BLUETOOTH_AMT
, elgbl_wifi_days_qty
, WIFI_AMT
, satellite_radio_amt
, elgbl_satellite_radio_days_qty
, SELLUP_AMT
, ADDL_TRUCK_PRODUCTS_AMT
, elgbl_coe_days_qty
, COE_AMT
, DW_CRTE_TSP
)
SELECT
TKT.RTN_MTH_KEY_ID
, TKT.OPN_IORG_ID
--	new changes begin January 2015		
, TKT.ORIG_BRAND_ID
--	new changes end January 2015			
, TKT.RENT_CNTRCT_NBR
, TKT.RATE_PLAN_NBR
, TKT.RATE_SRCE_ACCT_GID
, TKT.RATE_SRCE_CUST_NBR
, TKT.RATE_SRCE_CUST_SEQ_NBR
, TKT.SHOP_ACCT_GID
, TKT.SHOP_CUST_SEQ_NBR
, TKT.SHOP_CUST_NBR
, TKT.CNTRCT_CRED_EMP_ID
, TKT.CNTRCT_CRTE_EMP_ID
, TKT.RENT_TYP_CDE
, TKT.PCKUP_DTE
, TKT.RTN_DAY_DTE
, TKT.CLS_TKT_TSP
, TKT.GUI_RESV_NBR
, TKT.ORIG_RWRT_RENT_CNTRCT_NBR
, TKT.RENT_CNTRCT_TOT_DAY_CHRG_QTY
, TKT.TD_AMT
, TKT.ELGBL_DW_DAYS_QTY
, TKT.DW_AMT
, TKT.elgbl_pai_days_qty
, TKT.PAI_AMT
, TKT.elgbl_excess_days_qty
, TKT.EXCESS_AMT
, TKT.elgbl_slp_days_qty
, TKT.SLP_AMT
, TKT.elgbl_gps_days_qty
, TKT.GPS_AMT
, TKT.elgbl_rap_days_qty
, TKT.RAP_AMT
, TKT.elgbl_pre_paid_fuel_days_qty
, TKT.PRE_PAID_FUEL_AMT
, TKT.elgbl_bluetooth_days_qty
, TKT. BLUETOOTH_AMT
, TKT.ELGBL_WIFI_DAYS_QTY
, TKT.WIFI_AMT
, TKT.SATELLITE_RADIO_AMT
, TKT.ELGBL_SATELLITE_RADIO_DAYS_QTY
, COALESCE(SU.SELLUP,0.000) as SELLUP_AMT
, TKT.ADDL_TRUCK_PRODUCTS_AMT
, TKT.ELGBL_COE_DAYS_QTY
, TKT.COE_AMT
, CAST(CURRENT_TIMESTAMP as TIMESTAMP) as DW_TSP
FROM TKT_TEMP2 TKT
LEFT OUTER JOIN SELLUP_TMP SU
ON TKT.RENT_CNTRCT_NBR = SU.RENT_CNTRCT_NBR
UNION ALL
SELECT
FCT.RTN_MTH_KEY_ID
, FCT.OPN_IORG_ID
--	new changes begin January 2015		
, FCT.ORIG_BRAND_ID
--	new changes end January 2015			
, FCT.RENT_CNTRCT_NBR
, FCT.RATE_PLAN_NBR
, FCT.RATE_SRCE_ACCT_GID
, FCT.RATE_SRCE_CUST_NBR
, FCT.RATE_SRCE_CUST_SEQ_NBR
, FCT.SHOP_ACCT_GID
, FCT.SHOP_CUST_SEQ_NBR
, FCT.SHOP_CUST_NBR
, FCT.CNTRCT_CRED_EMP_ID
, FCT.CNTRCT_CRTE_EMP_ID
, FCT.RENT_TYP_CDE
, FCT.PCKUP_DTE
, FCT.RTN_DAY_DTE
, FCT.TKT_CLOS_TSP
, FCT.GUI_RESV_NBR
, FCT.ORIG_RWRT_RENT_CNTRCT_NBR
, FCT.RENT_DAYS_QTY
, FCT.TD_AMT
, FCT.ELGBL_DW_DAYS_QTY
, FCT.DW_AMT
, FCT.elgbl_pai_days_qty
, FCT.PAI_AMT
, FCT.elgbl_excess_days_qty
, FCT.EXCESS_AMT
, FCT.elgbl_slp_days_qty
, FCT.SLP_AMT
, FCT.elgbl_gps_days_qty
, FCT.GPS_AMT
, FCT.elgbl_rap_days_qty
, FCT.RAP_AMT
, FCT.elgbl_pre_paid_fuel_days_qty
, FCT.PRE_PAID_FUEL_AMT
, FCT.elgbl_bluetooth_days_qty
, FCT.BLUETOOTH_AMT
, FCT.ELGBL_WIFI_DAYS_QTY
, FCT.WIFI_AMT
, FCT.SATELLITE_RADIO_AMT
, FCT.ELGBL_SATELLITE_RADIO_DAYS_QTY
, FCT.SELLUP_AMT
, FCT.ADDL_TRUCK_PRODUCTS_AMT
, FCT.ELGBL_COE_DAYS_QTY
, FCT.COE_AMT
, FCT.DW_CRTE_TSP
FROM ECARS2_RPT.HOME_CITY_EMPL_CNTRCT_SALES FCT
INNER JOIN
ECARS2.OFC_DIR_BR ORG
ON FCT.OPN_IORG_ID = ORG.IORG_ID
AND ORG.CURR_VRSN_IND = 'Y'
AND ORG.ROW_STAT_CDE = 'A'
AND ORG.RECORD_STATUS = 'A'
CROSS JOIN
ECARS2_RPT_WT.ES_DATEPARMS DTE
WHERE
UPPER(TRIM(ORG.CNTRY_ISO_CDE)) NOT IN (${CNTRY_CDE})
OR
(  FCT.TKT_CLOS_TSP <
CAST((DTE.START_CST_DATE - 6 )AS TIMESTAMP FORMAT 'yyyy-MM-dd:HH:mm:ss')
OR
FCT.TKT_CLOS_TSP >= DTE.END_CST_DATE + ':00:00:00'
);
""").show(truncate=False) 

# COMMAND ----------
if xSqlState.ERROR_CODE != 0:
  
  dbutils.notebook.exit(xSqlState.ERROR_CODE)  


# COMMAND ----------
dbutils.notebook.exit(0)


# COMMAND ----------
!EOF!
bteq_rc=$?
#
# Check the return code and display an appropriate message to the console.
#
if [[ ${bteq_rc} != 0 ]]:

    
  
  spark.sql("""  
  echo "$APPL_NAM_ES: $CNTRY_CDE"  
  echo  
  echo "RUN STATUS:  ["FAILED"]"  
  echo "RETURN CODE: ["${bteq_rc}"]"  
  echo "LOG FILE:    ["${LOGFILE}"]"  
  echo "BTEQ END:    ["`date`"]"  
  exit 1  
  ELSE  
  echo "$APPL_NAM_ES: $CNTRY_CDE"  
  echo  
  echo "RUN STATUS:    ["SUCCESS"]"  
  echo "BTEQ END:      ["`date`"]"  
  fi  
  echo "LOG FILE: ["${LOGFILE}"]"  
  return ${bteq_rc}  
  --  
  -- End of Shell script.  
  --  
  exit 0  
  """).show(truncate=False) 