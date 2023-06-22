#Code converted on 2023-06-22 13:27:00
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark import SparkContext;
from pyspark import SparkConf
from pyspark.sql.session import SparkSession
from datetime import datetime
from dbruntime import dbutils
#from PySparkBQWriter import *
#import ProcessingUtils;
#bqw = PySparkBQWriter()
#bqw.setDebug(True)

conf = SparkConf().setMaster('local')
sc = SparkContext.getOrCreate(conf = conf)
spark = SparkSession(sc)

# Set global variables
starttime = datetime.now() #start timestamp of the script

# Read in job variables
# read_infa_paramfile('$PMRootDir\ParamFiles\Dell_infa_parms.txt', 'm_XLR8_STG_RES_V2') ProcessingUtils

# COMMAND ----------
# Variable_declaration_comment
dbutils.widgets.text(name='ODY_SYSDATE_GE', defaultValue='')

# COMMAND ----------
# Processing node LKP_DWH_STG_RES_FF_SRC, type SOURCE Prerequisite Lookup Object
# COLUMN COUNT: 3

LKP_DWH_STG_RES_FF_SRC = spark.sql(f"""SELECT DISTINCT
STG_RES_FF.FREQ_FLYER_CD AS FREQ_FLYER_CD, 
STG_RES_FF.RES_DOC_CD AS RES_DOC_CD 
FROM OSDWADMIN.STG_RES_FF STG_RES_FF
JOIN osdwadmin.dim_frequent_flyer dim_frequent_flyer
ON dim_frequent_flyer.frequent_flyer_cd=stg_res_ff.freq_flyer_cd
AND DIM_FREQUENT_FLYER.SYSTEM_ID=1""")
# Conforming fields names to the component layout
LKP_DWH_STG_RES_FF_SRC = LKP_DWH_STG_RES_FF_SRC \
	.withColumnRenamed(LKP_DWH_STG_RES_FF_SRC.columns[0],'IN_RES_DOC_CD') \
	.withColumnRenamed(LKP_DWH_STG_RES_FF_SRC.columns[1],'FREQ_FLYER_CD') \
	.withColumnRenamed(LKP_DWH_STG_RES_FF_SRC.columns[2],'RES_DOC_CD')

# COMMAND ----------
# Processing node LKP_FF_RRAS_PGM_ID_TERADATA_SRC, type SOURCE Prerequisite Lookup Object
# COLUMN COUNT: 3

LKP_FF_RRAS_PGM_ID_TERADATA_SRC = spark.sql(f"""SELECT	DISTINCT
FF_RRAS.TPR_PGM_ID AS TPR_PGM_ID,
FF_RRAS.RES_RES_NBR AS RES_RES_NBR 
FROM ODY_RV.FF_RRAS FF_RRAS
JOIN INTGRT_RPT.DIM_FREQUENT_FLYER DIM_FREQUENT_FLYER
	ON DIM_FREQUENT_FLYER.FREQUENT_FLYER_CD=FF_RRAS.TPR_PGM_ID
	and DIM_FREQUENT_FLYER.SYSTEM_ID=1
WHERE FF_RRAS.TPR_PGM_ID IS NOT NULL 
	AND FF_RRAS.RES_RES_NBR > 0
	AND ROW_STAT_CDE='A'
		AND FF_RRAS.SRCE_EFF_TSP > CURRENT_DATE -90
ORDER BY FF_RRAS.RES_RES_NBR, FF_RRAS.SRCE_EFF_TSP DESC """)
# Conforming fields names to the component layout
LKP_FF_RRAS_PGM_ID_TERADATA_SRC = LKP_FF_RRAS_PGM_ID_TERADATA_SRC \
	.withColumnRenamed(LKP_FF_RRAS_PGM_ID_TERADATA_SRC.columns[0],'IN_res_res_nbr') \
	.withColumnRenamed(LKP_FF_RRAS_PGM_ID_TERADATA_SRC.columns[1],'res_res_nbr') \
	.withColumnRenamed(LKP_FF_RRAS_PGM_ID_TERADATA_SRC.columns[2],'tpr_pgm_id')

# COMMAND ----------
# Processing node LKP_ODY_FF_RRAS_TD_SRC, type SOURCE Prerequisite Lookup Object
# COLUMN COUNT: 3

LKP_ODY_FF_RRAS_TD_SRC = spark.sql(f"""SELECT MAX(FF_RRAS.mbr_nbr) as mbr_nbr, 
FF_RRAS.res_res_nbr as res_res_nbr 
FROM ODY_RV.FF_RRAS
WHERE TPR_PGM_ID IS NOT NULL 
AND RES_RES_NBR > 0
AND ROW_STAT_CDE='A'
AND MBR_NBR IS NOT NULL
AND MBR_NBR <> ' '
AND srce_eff_tsp > CURRENT_DATE -365
GROUP BY res_res_nbr 
ORDER BY ff_RRAS.res_res_nbr """)
# Conforming fields names to the component layout
LKP_ODY_FF_RRAS_TD_SRC = LKP_ODY_FF_RRAS_TD_SRC \
	.withColumnRenamed(LKP_ODY_FF_RRAS_TD_SRC.columns[0],'IN_res_res_nbr') \
	.withColumnRenamed(LKP_ODY_FF_RRAS_TD_SRC.columns[1],'mbr_nbr') \
	.withColumnRenamed(LKP_ODY_FF_RRAS_TD_SRC.columns[2],'res_res_nbr')

# COMMAND ----------
# Processing node LKP_SPECIAL_EQUIPMENT_SRC, type SOURCE Prerequisite Lookup Object
# COLUMN COUNT: 5

LKP_SPECIAL_EQUIPMENT_SRC = spark.sql(f"""SELECT
RES_NUMBER,
POSITION,
SPCL_EQUIP_CD
FROM STG_RES_SPCL_EQUIP""")
# Conforming fields names to the component layout
LKP_SPECIAL_EQUIPMENT_SRC = LKP_SPECIAL_EQUIPMENT_SRC \
	.withColumnRenamed(LKP_SPECIAL_EQUIPMENT_SRC.columns[0],'IN_RES_NUMBER') \
	.withColumnRenamed(LKP_SPECIAL_EQUIPMENT_SRC.columns[1],'IN_POSITION') \
	.withColumnRenamed(LKP_SPECIAL_EQUIPMENT_SRC.columns[2],'RES_NUMBER') \
	.withColumnRenamed(LKP_SPECIAL_EQUIPMENT_SRC.columns[3],'POSITION') \
	.withColumnRenamed(LKP_SPECIAL_EQUIPMENT_SRC.columns[4],'SPCL_EQUIP_CD')

# COMMAND ----------
# Processing node LKP_SPECIAL_EQUIP_QTY_SRC, type SOURCE Prerequisite Lookup Object
# COLUMN COUNT: 5

LKP_SPECIAL_EQUIP_QTY_SRC = spark.sql(f"""SELECT
RES_NUMBER,
POSITION,
SPCL_EQUIP_QTY
FROM STG_RES_SPCL_EQUIP""")
# Conforming fields names to the component layout
LKP_SPECIAL_EQUIP_QTY_SRC = LKP_SPECIAL_EQUIP_QTY_SRC \
	.withColumnRenamed(LKP_SPECIAL_EQUIP_QTY_SRC.columns[0],'IN_RES_NUMBER') \
	.withColumnRenamed(LKP_SPECIAL_EQUIP_QTY_SRC.columns[1],'IN_POSITION') \
	.withColumnRenamed(LKP_SPECIAL_EQUIP_QTY_SRC.columns[2],'RES_NUMBER') \
	.withColumnRenamed(LKP_SPECIAL_EQUIP_QTY_SRC.columns[3],'POSITION') \
	.withColumnRenamed(LKP_SPECIAL_EQUIP_QTY_SRC.columns[4],'SPCL_EQUIP_QTY')

# COMMAND ----------
# Processing node LKP_SPECIAL_EQUIP_VALID_SRC, type SOURCE Prerequisite Lookup Object
# COLUMN COUNT: 5

LKP_SPECIAL_EQUIP_VALID_SRC = spark.sql(f"""SELECT
RES_NUMBER,
POSITION,
VALID_EQUIP_CD
FROM STG_RES_SPCL_EQUIP""")
# Conforming fields names to the component layout
LKP_SPECIAL_EQUIP_VALID_SRC = LKP_SPECIAL_EQUIP_VALID_SRC \
	.withColumnRenamed(LKP_SPECIAL_EQUIP_VALID_SRC.columns[0],'IN_RES_NUMBER') \
	.withColumnRenamed(LKP_SPECIAL_EQUIP_VALID_SRC.columns[1],'IN_POSITION') \
	.withColumnRenamed(LKP_SPECIAL_EQUIP_VALID_SRC.columns[2],'RES_NUMBER') \
	.withColumnRenamed(LKP_SPECIAL_EQUIP_VALID_SRC.columns[3],'POSITION') \
	.withColumnRenamed(LKP_SPECIAL_EQUIP_VALID_SRC.columns[4],'VALID_EQUIP_CD')

# COMMAND ----------
# Processing node LKP_VALIDATE_DW_DIM_TURNDOWN_SRC, type SOURCE Prerequisite Lookup Object
# COLUMN COUNT: 3

LKP_VALIDATE_DW_DIM_TURNDOWN_SRC = spark.sql(f"""SELECT
TURNDOWN_CD,
TURNDOWN_ID
FROM DIM_TURNDOWN""")
# Conforming fields names to the component layout
LKP_VALIDATE_DW_DIM_TURNDOWN_SRC = LKP_VALIDATE_DW_DIM_TURNDOWN_SRC \
	.withColumnRenamed(LKP_VALIDATE_DW_DIM_TURNDOWN_SRC.columns[0],'IN_TURNDOWN_CD') \
	.withColumnRenamed(LKP_VALIDATE_DW_DIM_TURNDOWN_SRC.columns[1],'TURNDOWN_CD') \
	.withColumnRenamed(LKP_VALIDATE_DW_DIM_TURNDOWN_SRC.columns[2],'TURNDOWN_ID')

# COMMAND ----------
# Processing node LKP_WRAP_UP_TYPE_SRC, type SOURCE Prerequisite Lookup Object
# COLUMN COUNT: 3

LKP_WRAP_UP_TYPE_SRC = spark.sql(f"""SELECT
WRAP_UP_CD,
RMS_WRAP_UP_TYP
FROM GWYDB.WRAP_UP_DEFS""")
# Conforming fields names to the component layout
LKP_WRAP_UP_TYPE_SRC = LKP_WRAP_UP_TYPE_SRC \
	.withColumnRenamed(LKP_WRAP_UP_TYPE_SRC.columns[0],'IN_WRAP_UP_CD') \
	.withColumnRenamed(LKP_WRAP_UP_TYPE_SRC.columns[1],'WRAP_UP_CD') \
	.withColumnRenamed(LKP_WRAP_UP_TYPE_SRC.columns[2],'RMS_WRAP_UP_TYP')

# COMMAND ----------
# Processing node SHORTCUT_TO_LKP_DIM_BRAND_GDS_SRC, type SOURCE Prerequisite Lookup Object
# COLUMN COUNT: 3

SHORTCUT_TO_LKP_DIM_BRAND_GDS_SRC = spark.sql(f"""SELECT
BRAND_GDS_CD,
BRAND_ID
FROM DIM_BRAND""")
# Conforming fields names to the component layout
SHORTCUT_TO_LKP_DIM_BRAND_GDS_SRC = SHORTCUT_TO_LKP_DIM_BRAND_GDS_SRC \
	.withColumnRenamed(SHORTCUT_TO_LKP_DIM_BRAND_GDS_SRC.columns[0],'IN_BRAND_GDS_ID') \
	.withColumnRenamed(SHORTCUT_TO_LKP_DIM_BRAND_GDS_SRC.columns[1],'BRAND_GDS_CD') \
	.withColumnRenamed(SHORTCUT_TO_LKP_DIM_BRAND_GDS_SRC.columns[2],'BRAND_ID')

# COMMAND ----------
# Processing node SHORTCUT_TO_LKP_INT_DEBIT_CARD_BINS_SRC, type SOURCE Prerequisite Lookup Object
# COLUMN COUNT: 3

SHORTCUT_TO_LKP_INT_DEBIT_CARD_BINS_SRC = spark.sql(f"""SELECT
BIN_LO_NUMBER,
BIN_HI_NUMBER
FROM INT_DEBIT_CARD_BINS""")
# Conforming fields names to the component layout
SHORTCUT_TO_LKP_INT_DEBIT_CARD_BINS_SRC = SHORTCUT_TO_LKP_INT_DEBIT_CARD_BINS_SRC \
	.withColumnRenamed(SHORTCUT_TO_LKP_INT_DEBIT_CARD_BINS_SRC.columns[0],'IN_CREDIT_CARD') \
	.withColumnRenamed(SHORTCUT_TO_LKP_INT_DEBIT_CARD_BINS_SRC.columns[1],'BIN_LO_NUMBER') \
	.withColumnRenamed(SHORTCUT_TO_LKP_INT_DEBIT_CARD_BINS_SRC.columns[2],'BIN_HI_NUMBER')

# COMMAND ----------
# Processing node SHORTCUT_TO_LKP_ODY_EC_STAFF_SRC, type SOURCE Prerequisite Lookup Object
# COLUMN COUNT: 3

SHORTCUT_TO_LKP_ODY_EC_STAFF_SRC = spark.sql(f"""SELECT
STAFF_ID,
UNIX_USR
FROM GWYDB.EC_STAFF""")
# Conforming fields names to the component layout
SHORTCUT_TO_LKP_ODY_EC_STAFF_SRC = SHORTCUT_TO_LKP_ODY_EC_STAFF_SRC \
	.withColumnRenamed(SHORTCUT_TO_LKP_ODY_EC_STAFF_SRC.columns[0],'IN_STAFF_ID') \
	.withColumnRenamed(SHORTCUT_TO_LKP_ODY_EC_STAFF_SRC.columns[1],'STAFF_ID') \
	.withColumnRenamed(SHORTCUT_TO_LKP_ODY_EC_STAFF_SRC.columns[2],'UNIX_USR')

# COMMAND ----------
# Processing node SHORTCUT_TO_LKP_ODY_VALIDATE_MOP_SRC, type SOURCE Prerequisite Lookup Object
# COLUMN COUNT: 2

SHORTCUT_TO_LKP_ODY_VALIDATE_MOP_SRC = spark.sql(f"""SELECT
MOP_CD
FROM GWYDB.MTHD_OF_PYMTS""")
# Conforming fields names to the component layout
SHORTCUT_TO_LKP_ODY_VALIDATE_MOP_SRC = SHORTCUT_TO_LKP_ODY_VALIDATE_MOP_SRC \
	.withColumnRenamed(SHORTCUT_TO_LKP_ODY_VALIDATE_MOP_SRC.columns[0],'IN_MOP_CD') \
	.withColumnRenamed(SHORTCUT_TO_LKP_ODY_VALIDATE_MOP_SRC.columns[1],'MOP_CD')

# COMMAND ----------
# Processing node SHORTCUT_TO_LKP_ODY_VALIDATE_MOP_MOP_SRC, type SOURCE Prerequisite Lookup Object
# COLUMN COUNT: 2

SHORTCUT_TO_LKP_ODY_VALIDATE_MOP_MOP_SRC = spark.sql(f"""SELECT
MOP_TYP_CD
FROM GWYDB.MOP_TYPS""")
# Conforming fields names to the component layout
SHORTCUT_TO_LKP_ODY_VALIDATE_MOP_MOP_SRC = SHORTCUT_TO_LKP_ODY_VALIDATE_MOP_MOP_SRC \
	.withColumnRenamed(SHORTCUT_TO_LKP_ODY_VALIDATE_MOP_MOP_SRC.columns[0],'IN_MOP_TYP') \
	.withColumnRenamed(SHORTCUT_TO_LKP_ODY_VALIDATE_MOP_MOP_SRC.columns[1],'MOP_TYP_CD')

# COMMAND ----------
# Processing node SHORTCUT_TO_LKP_ODY_VALIDATE_VEH_CLASS_SRC, type SOURCE Prerequisite Lookup Object
# COLUMN COUNT: 2

SHORTCUT_TO_LKP_ODY_VALIDATE_VEH_CLASS_SRC = spark.sql(f"""SELECT
CAT_CD
FROM GWYDB.VHCL_CATS""")
# Conforming fields names to the component layout
SHORTCUT_TO_LKP_ODY_VALIDATE_VEH_CLASS_SRC = SHORTCUT_TO_LKP_ODY_VALIDATE_VEH_CLASS_SRC \
	.withColumnRenamed(SHORTCUT_TO_LKP_ODY_VALIDATE_VEH_CLASS_SRC.columns[0],'IN_CAT_CD') \
	.withColumnRenamed(SHORTCUT_TO_LKP_ODY_VALIDATE_VEH_CLASS_SRC.columns[1],'CAT_CD')

# COMMAND ----------
# Processing node LKP_EXT_PRODUCT_CODE_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 3

LKP_EXT_PRODUCT_CODE_SRC = spark.sql(f"""SELECT GWYDB.AVG_DLY_RATES.RES_PEPC_EPROD_ID as RES_PEPC_EPROD_ID, GWYDB.AVG_DLY_RATES.RES_RES_NBR as RES_RES_NBR 
FROM GWYDB.AVG_DLY_RATES
where res_pepc_eprod_id is not null
ORDER BY RES_RES_NBR, CREATE_DATE DESC """)
# Conforming fields names to the component layout
LKP_EXT_PRODUCT_CODE_SRC = LKP_EXT_PRODUCT_CODE_SRC \
	.withColumnRenamed(LKP_EXT_PRODUCT_CODE_SRC.columns[0],'IN_RES_NBR') \
	.withColumnRenamed(LKP_EXT_PRODUCT_CODE_SRC.columns[1],'RES_RES_NBR') \
	.withColumnRenamed(LKP_EXT_PRODUCT_CODE_SRC.columns[2],'RES_PEPC_EPROD_ID')

# COMMAND ----------
# Processing node LKP_ODY_GET_STN_COUNTRY_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 3

LKP_ODY_GET_STN_COUNTRY_SRC = spark.sql(f"""SELECT
STN_ID,
CRY_ARIMP_CRY_CD
FROM GWYDB.STNS""")
# Conforming fields names to the component layout
LKP_ODY_GET_STN_COUNTRY_SRC = LKP_ODY_GET_STN_COUNTRY_SRC \
	.withColumnRenamed(LKP_ODY_GET_STN_COUNTRY_SRC.columns[0],'IN_STATION_CD') \
	.withColumnRenamed(LKP_ODY_GET_STN_COUNTRY_SRC.columns[1],'STN_ID') \
	.withColumnRenamed(LKP_ODY_GET_STN_COUNTRY_SRC.columns[2],'CRY_ARIMP_CRY_CD')

# COMMAND ----------
# Processing node SHORTCUT_TO_LKP_ODY_VALIDATE_STATION1_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 0

SHORTCUT_TO_LKP_ODY_VALIDATE_STATION1_SRC = spark.sql(f"""SELECT
STN_ID
FROM GWYDB.STNS""")

# COMMAND ----------
# Processing node SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON2_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 0

SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON2_SRC = spark.sql(f"""SELECT
COUPON_ID
FROM GWYDB.COUP_PROMO_VALD""")

# COMMAND ----------
# Processing node LKP_RES_CALLER_INFO_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 4

LKP_RES_CALLER_INFO_SRC = spark.sql(f"""SELECT a.CLR_EMAIL_ADDR as CLR_EMAIL_ADDR, a.AGENT_EMAIL_ADDRESS as AGENT_EMAIL_ADDRESS, a.RES_NBR as RES_NBR 
FROM GWYDB.RES_CLR_INFO a, COGRPT.STG_RES_ODY b
WHERE a.RES_NBR = b.RES_NBR
ORDER BY RES_NBR """)
# Conforming fields names to the component layout
LKP_RES_CALLER_INFO_SRC = LKP_RES_CALLER_INFO_SRC \
	.withColumnRenamed(LKP_RES_CALLER_INFO_SRC.columns[0],'IN_RES_NBR') \
	.withColumnRenamed(LKP_RES_CALLER_INFO_SRC.columns[1],'RES_NBR') \
	.withColumnRenamed(LKP_RES_CALLER_INFO_SRC.columns[2],'CLR_EMAIL_ADDR') \
	.withColumnRenamed(LKP_RES_CALLER_INFO_SRC.columns[3],'AGENT_EMAIL_ADDRESS')

# COMMAND ----------
# Processing node SHORTCUT_TO_LKP_ODY_VALIDATE_ACCOUNT_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 2

SHORTCUT_TO_LKP_ODY_VALIDATE_ACCOUNT_SRC = spark.sql(f"""SELECT
CON_ID
FROM GWYDB.CONS""")
# Conforming fields names to the component layout
SHORTCUT_TO_LKP_ODY_VALIDATE_ACCOUNT_SRC = SHORTCUT_TO_LKP_ODY_VALIDATE_ACCOUNT_SRC \
	.withColumnRenamed(SHORTCUT_TO_LKP_ODY_VALIDATE_ACCOUNT_SRC.columns[0],'IN_CON_ID') \
	.withColumnRenamed(SHORTCUT_TO_LKP_ODY_VALIDATE_ACCOUNT_SRC.columns[1],'CON_ID')

# COMMAND ----------
# Processing node SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON1_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 0

SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON1_SRC = spark.sql(f"""SELECT
COUPON_ID
FROM GWYDB.COUP_PROMO_VALD""")

# COMMAND ----------
# Processing node SHORTCUT_TO_LKP_ODY_VALIDATE_PRODUCT_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 4

SHORTCUT_TO_LKP_ODY_VALIDATE_PRODUCT_SRC = spark.sql(f"""SELECT
PROD_INST_CD,
VER_NBR
FROM GWYDB.PROD_INSTS""")
# Conforming fields names to the component layout
SHORTCUT_TO_LKP_ODY_VALIDATE_PRODUCT_SRC = SHORTCUT_TO_LKP_ODY_VALIDATE_PRODUCT_SRC \
	.withColumnRenamed(SHORTCUT_TO_LKP_ODY_VALIDATE_PRODUCT_SRC.columns[0],'IN_PROD_INST_CD') \
	.withColumnRenamed(SHORTCUT_TO_LKP_ODY_VALIDATE_PRODUCT_SRC.columns[1],'IN_VER_NBR') \
	.withColumnRenamed(SHORTCUT_TO_LKP_ODY_VALIDATE_PRODUCT_SRC.columns[2],'PROD_INST_CD') \
	.withColumnRenamed(SHORTCUT_TO_LKP_ODY_VALIDATE_PRODUCT_SRC.columns[3],'VER_NBR')

# COMMAND ----------
# Processing node SHORTCUT_TO_LKP_ODY_VALIDATE_STATION_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 2

SHORTCUT_TO_LKP_ODY_VALIDATE_STATION_SRC = spark.sql(f"""SELECT
STN_ID
FROM GWYDB.STNS""")
# Conforming fields names to the component layout
SHORTCUT_TO_LKP_ODY_VALIDATE_STATION_SRC = SHORTCUT_TO_LKP_ODY_VALIDATE_STATION_SRC \
	.withColumnRenamed(SHORTCUT_TO_LKP_ODY_VALIDATE_STATION_SRC.columns[0],'IN_STN_ID') \
	.withColumnRenamed(SHORTCUT_TO_LKP_ODY_VALIDATE_STATION_SRC.columns[1],'STN_ID')

# COMMAND ----------
# Processing node SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 2

SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON_SRC = spark.sql(f"""SELECT
COUPON_ID
FROM GWYDB.COUP_PROMO_VALD""")
# Conforming fields names to the component layout
SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON_SRC = SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON_SRC \
	.withColumnRenamed(SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON_SRC.columns[0],'IN_COUPON_ID') \
	.withColumnRenamed(SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON_SRC.columns[1],'COUPON_ID')

# COMMAND ----------
# Processing node LKP_CRY_CURR_BRND_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 13

LKP_CRY_CURR_BRND_SRC = spark.sql(f"""SELECT
CRY_CURR_BRND_ID,
CRY_ARIMP_CRY_CD,
BRN_BRAND_ID,
CUR_CURR_CD,
RECORD_STATUS
FROM GWYDB.CRY_CURR_BRND""")
# Conforming fields names to the component layout
LKP_CRY_CURR_BRND_SRC = LKP_CRY_CURR_BRND_SRC \
	.withColumnRenamed(LKP_CRY_CURR_BRND_SRC.columns[0],'CRY_CURR_BRND_ID') \
	.withColumnRenamed(LKP_CRY_CURR_BRND_SRC.columns[1],'CRY_ARIMP_CRY_CD') \
	.withColumnRenamed(LKP_CRY_CURR_BRND_SRC.columns[2],'BRN_BRAND_ID') \
	.withColumnRenamed(LKP_CRY_CURR_BRND_SRC.columns[3],'CUR_CURR_CD') \
	.withColumnRenamed(LKP_CRY_CURR_BRND_SRC.columns[4],'RECORD_STATUS') \
	.withColumnRenamed(LKP_CRY_CURR_BRND_SRC.columns[5],'CREATED_BY') \
	.withColumnRenamed(LKP_CRY_CURR_BRND_SRC.columns[6],'CREATE_MODULE') \
	.withColumnRenamed(LKP_CRY_CURR_BRND_SRC.columns[7],'CREATE_TIMESTAMP') \
	.withColumnRenamed(LKP_CRY_CURR_BRND_SRC.columns[8],'UPDATED_BY') \
	.withColumnRenamed(LKP_CRY_CURR_BRND_SRC.columns[9],'UPDATE_MODULE') \
	.withColumnRenamed(LKP_CRY_CURR_BRND_SRC.columns[10],'UPDATE_TIMESTAMP') \
	.withColumnRenamed(LKP_CRY_CURR_BRND_SRC.columns[11],'IN_COUNTRY_CD') \
	.withColumnRenamed(LKP_CRY_CURR_BRND_SRC.columns[12],'IN_BRN_BRAND_ID')

# COMMAND ----------
# Processing node LKP_SPCL_INST_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 4

LKP_SPCL_INST_SRC = spark.sql(f"""SELECT
SPCL_INSTRS,
CASH_IN_CLUB_NBR,
RES_DOC_CD
FROM STG_RES_SPCL_INST""")
# Conforming fields names to the component layout
LKP_SPCL_INST_SRC = LKP_SPCL_INST_SRC \
	.withColumnRenamed(LKP_SPCL_INST_SRC.columns[0],'IN_RES_DOC_CD') \
	.withColumnRenamed(LKP_SPCL_INST_SRC.columns[1],'RES_DOC_CD') \
	.withColumnRenamed(LKP_SPCL_INST_SRC.columns[2],'SPCL_INSTRS') \
	.withColumnRenamed(LKP_SPCL_INST_SRC.columns[3],'CASH_IN_CLUB_NBR')

# COMMAND ----------
# Processing node SQ_Shortcut_To_RES, type SOURCE 
# COLUMN COUNT: 71

SQ_Shortcut_To_RES = spark.sql(f"""SELECT  /*+ ORDERED USE_NL(RES)*/
RES.RES_NBR, RES.LST_CHNG_DT, RES.SLC_SVC_LEV_CD, RES.STA_STN_ID_RES_TAKEN, RES.BAC_ACCT_ID, RES.CANCEL_AMT, 
       RES.CANCEL_TMSP, RES.CLR_NAME, RES.COUPON_1_ID, RES.COUPON_1_QTY, RES.COUPON_2_ID, RES.COUPON_2_QTY, RES.COUPON_3_ID, 
	   RES.COUPON_3_QTY, UPPER(RES.CREAT_BY), RES.CRSC_GDS_CD, RES.CTR_CON_ID_COMM, RES.CTR_CON_ID_PRIM, RES.CTR_CON_ID_RENT, 
	   RES.DRV_DVR_ID, RES.DVR_FRST_NM, RES.DVR_SRNM, RES.DISC_PCT, RES.EST_STAFF_ID_BEHALF_OF, RES.EXTRNL_REF, 
RES.EXTRNL_VCHR_ID,
RES.FF_FLG, RES.FLGT_NBR, RES.GUAR_STAT, 
	   RES.MOP_MOP_CD_PTNTLY, RES.MPT_MOP_TYP_CD, RES.NCR_NBR, RES.NO_SHOW_AMT, RES.ORG_ID_BKR, RES.ORT_ROLE_TYP_CD_BKR, RES.PNR_RCD_LOC, 
	   RES.PRC_PROD_INST_CD, RES.PRC_VER_NBR, RES.PROJ_CI_TMSP, RES.PROJ_CO_TMSP, RES.PRPY_FLG, RES.PRPY_MIN_AMT, RES.RES_SRC_CD, 
	   RES.RES_STAT, RES.RES_TMSP, RES.SEAT_PRICE_AMT, RES.SPCL_EQP_FLG, RES.STA_STN_ID_CHECKIN, RES.STA_STN_ID_CHECKOUT, 
	   RES.TDD_TRNDN_RSN_CD, RES.VCA_CAT_CD_ACTL, RES.VCA_CAT_CD_DRVN, RES.VHCL_GRP_STOPSL_OVRD, RES.WRD_WRAP_UP_CD, 
	   RES.MARKET_SEGMENT_CD, 
gwydb.fn_rpt_get_notes(res.NTE_NOTE_ID) as DW_COMMENTS,
RES.BRN_BRAND_ID, RES.WEB_SRC_CD, RES.POS_COUNTRY_CD, RES.E_VOUCHER,  RES.PROD_CURR_SEAT_PRICE, RES.PROD_CURR_CD,
RES.PTP_ELIGIBLE_FLG, RES.MEMBERSHIP_ID, RES.SUB_CHNL_SRC_CD, RES.CTR_CON_ID_PRFR, 
RES.CARD_NBR_FSIX, RES.CARD_NBR_LFOUR, 
RES.NET_REV_AMT, RES.LOYALTY_TRANS_ID,
RES.COUPON_FIXED_AMT
FROM
  COGRPT.STG_INVOICED_RENTALS, GWYDB.RES
WHERE  
 RES.RES_NBR = STG_INVOICED_RENTALS.RES_RES_NBR""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node LKP_EXT_PRODUCT_CODE, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3


LKP_EXT_PRODUCT_CODE_lookup_result = SQ_Shortcut_To_RES.select( \
	SQ_Shortcut_To_RES.sys_row_id.alias('sys_row_id'), \
	SQ_Shortcut_To_RES.RES_NBR.alias('IN_RES_NBR')).join(LKP_EXT_PRODUCT_CODE_SRC, (col('RES_RES_NBR') == col('IN_RES_NBR')), 'left')
LKP_EXT_PRODUCT_CODE = LKP_EXT_PRODUCT_CODE_lookup_result.select( \
	LKP_EXT_PRODUCT_CODE_lookup_result.sys_row_id, \
	col('RES_PEPC_EPROD_ID') \
)

# COMMAND ----------
# Processing node LKP_ODY_GET_STN_COUNTRY, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3


LKP_ODY_GET_STN_COUNTRY_lookup_result = SQ_Shortcut_To_RES.select( \
	SQ_Shortcut_To_RES.sys_row_id.alias('sys_row_id'), \
	SQ_Shortcut_To_RES.STA_STN_ID_CHECKOUT.alias('IN_STATION_CD')).join(LKP_ODY_GET_STN_COUNTRY_SRC, (col('STN_ID') == col('IN_STATION_CD')), 'left')
LKP_ODY_GET_STN_COUNTRY = LKP_ODY_GET_STN_COUNTRY_lookup_result.select( \
	LKP_ODY_GET_STN_COUNTRY_lookup_result.sys_row_id, \
	col('CRY_ARIMP_CRY_CD') \
)

# COMMAND ----------
# Processing node SHORTCUT_TO_LKP_ODY_VALIDATE_STATION1, type LOOKUP_FROM_PRECACHED_DATASET 
# COLUMN COUNT: 0


SHORTCUT_TO_LKP_ODY_VALIDATE_STATION1_lookup_result = SQ_Shortcut_To_RES.join(SHORTCUT_TO_LKP_ODY_VALIDATE_STATION1_SRC, (col('STN_ID') == col('IN_STN_ID')), 'left')
.withColumn('row_num_STN_ID', row_number().over(partitionBy("sys_row_id").orderBy("STN_ID")))
SHORTCUT_TO_LKP_ODY_VALIDATE_STATION1 = SHORTCUT_TO_LKP_ODY_VALIDATE_STATION1_lookup_result.filter(col("row_num_STN_ID") == 1).select( \
	SHORTCUT_TO_LKP_ODY_VALIDATE_STATION1_lookup_result.sys_row_id \
)

# COMMAND ----------
# Processing node SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON2, type LOOKUP_FROM_PRECACHED_DATASET 
# COLUMN COUNT: 0


SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON2_lookup_result = SQ_Shortcut_To_RES.join(SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON2_SRC, (col('COUPON_ID') == col('IN_COUPON_ID')), 'left')
.withColumn('row_num_COUPON_ID', row_number().over(partitionBy("sys_row_id").orderBy("COUPON_ID")))
SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON2 = SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON2_lookup_result.filter(col("row_num_COUPON_ID") == 1).select( \
	SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON2_lookup_result.sys_row_id \
)

# COMMAND ----------
# Processing node LKP_RES_CALLER_INFO, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4


LKP_RES_CALLER_INFO_lookup_result = SQ_Shortcut_To_RES.select( \
	SQ_Shortcut_To_RES.sys_row_id.alias('sys_row_id'), \
	SQ_Shortcut_To_RES.RES_NBR.alias('IN_RES_NBR')).join(LKP_RES_CALLER_INFO_SRC, (col('RES_NBR') == col('IN_RES_NBR')), 'left')
LKP_RES_CALLER_INFO = LKP_RES_CALLER_INFO_lookup_result.select( \
	LKP_RES_CALLER_INFO_lookup_result.sys_row_id, \
	col('CLR_EMAIL_ADDR'), \
	col('AGENT_EMAIL_ADDRESS') \
)

# COMMAND ----------
# Processing node SHORTCUT_TO_LKP_ODY_VALIDATE_ACCOUNT, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 2


SHORTCUT_TO_LKP_ODY_VALIDATE_ACCOUNT_lookup_result = SQ_Shortcut_To_RES.select( \
	SQ_Shortcut_To_RES.sys_row_id.alias('sys_row_id'), \
	SQ_Shortcut_To_RES.CTR_CON_ID_PRIM.alias('IN_CON_ID')).join(SHORTCUT_TO_LKP_ODY_VALIDATE_ACCOUNT_SRC, (col('CON_ID') == col('IN_CON_ID')), 'left')
SHORTCUT_TO_LKP_ODY_VALIDATE_ACCOUNT = SHORTCUT_TO_LKP_ODY_VALIDATE_ACCOUNT_lookup_result.select( \
	SHORTCUT_TO_LKP_ODY_VALIDATE_ACCOUNT_lookup_result.sys_row_id, \
	col('IN_CON_ID'), \
	col('CON_ID') \
)

# COMMAND ----------
# Processing node SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON1, type LOOKUP_FROM_PRECACHED_DATASET 
# COLUMN COUNT: 0


SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON1_lookup_result = SQ_Shortcut_To_RES.join(SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON1_SRC, (col('COUPON_ID') == col('IN_COUPON_ID')), 'left')
.withColumn('row_num_COUPON_ID', row_number().over(partitionBy("sys_row_id").orderBy("COUPON_ID")))
SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON1 = SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON1_lookup_result.filter(col("row_num_COUPON_ID") == 1).select( \
	SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON1_lookup_result.sys_row_id \
)

# COMMAND ----------
# Processing node SHORTCUT_TO_LKP_ODY_VALIDATE_PRODUCT, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4


SHORTCUT_TO_LKP_ODY_VALIDATE_PRODUCT_lookup_result = SQ_Shortcut_To_RES.select( \
	SQ_Shortcut_To_RES.sys_row_id.alias('sys_row_id'), \
	SQ_Shortcut_To_RES.PRC_PROD_INST_CD.alias('IN_PROD_INST_CD'), \
	SQ_Shortcut_To_RES.PRC_VER_NBR.alias('IN_VER_NBR')).join(SHORTCUT_TO_LKP_ODY_VALIDATE_PRODUCT_SRC, (col('PROD_INST_CD') == col('IN_PROD_INST_CD')) & (col('VER_NBR') == col('IN_VER_NBR')), 'left')
SHORTCUT_TO_LKP_ODY_VALIDATE_PRODUCT = SHORTCUT_TO_LKP_ODY_VALIDATE_PRODUCT_lookup_result.select( \
	SHORTCUT_TO_LKP_ODY_VALIDATE_PRODUCT_lookup_result.sys_row_id, \
	col('IN_PROD_INST_CD'), \
	col('IN_VER_NBR'), \
	col('PROD_INST_CD'), \
	col('VER_NBR') \
)

# COMMAND ----------
# Processing node Shortcut_to_EXP_RES_SRC, type EXPRESSION 
# COLUMN COUNT: 8

Shortcut_to_EXP_RES_SRC = SQ_Shortcut_To_RES.select( \
	SQ_Shortcut_To_RES.sys_row_id.alias('sys_row_id'), \
	SQ_Shortcut_To_RES.RES_SRC_CD.alias('RES_SRC_CD'), \
	SQ_Shortcut_To_RES.CRSC_GDS_CD.alias('CRSC_GDS_CD'), \
	SQ_Shortcut_To_RES.STA_STN_ID_RES_TAKEN.alias('STA_STN_ID_RES_TAKEN'), \
	SQ_Shortcut_To_RES.WEB_SRC_CD.alias('WEB_SRC_CD'), \
	SQ_Shortcut_To_RES.CREAT_BY.alias('CREAT_BY'), \
	SQ_Shortcut_To_RES.BRN_BRAND_ID.alias('BRN_BRAND_ID'), \
	SQ_Shortcut_To_RES.SUB_CHNL_SRC_CD.alias('SUB_CHNL_SRC_CD'), \
	(when((col('RES_SRC_CD') == lit('LNK')) AND(col('SUB_CHNL_SRC_CD').isNotNull()) , col('SUB_CHNL_SRC_CD')).otherwise(when((col('RES_SRC_CD') == lit('GDS')) AND(col('CRSC_GDS_CD').isNull()) AND(col('STA_STN_ID_RES_TAKEN') == lit('WWWC66')) , lit('WEB') , when((col('RES_SRC_CD') == lit('GDS')) AND(col('CRSC_GDS_CD').isNull()) , lit('SCR') , when((col('CRSC_GDS_CD').isNull()) AND(col('CREAT_BY') == lit('GALILEO')) , lit('1G') , when((col('CRSC_GDS_CD').isNotNull()) AND(col('RES_SRC_CD') == lit('GDS')) , col('CRSC_GDS_CD') , when((col('RES_SRC_CD') == lit('WEB')) AND(col('CREAT_BY') == lit('ALAMO_MOBILE')) , lit('MOBILE') , when((col('RES_SRC_CD') == lit('WEB')) AND(col('CREAT_BY') == lit('ENT_MOBILE')) , lit('MOBILE') , when((col('RES_SRC_CD') == lit('WEB')) AND(col('CREAT_BY') == lit('ENT_WEB')) , lit('WEB') , when((col('RES_SRC_CD') == lit('WEB')) AND(col('WEB_SRC_CD').isNotNull()) , col('WEB_SRC_CD') , when((col('RES_SRC_CD') == lit('SCR')) AND(col('WEB_SRC_CD') == lit('VCIRES')) , col('WEB_SRC_CD') , when(col('RES_SRC_CD') != lit('GDS') , col('RES_SRC_CD'))))))))))))).alias('OUT_DW_RES_SRC_CD') \
)

# COMMAND ----------
# Processing node SHORTCUT_TO_LKP_ODY_VALIDATE_STATION, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 2


SHORTCUT_TO_LKP_ODY_VALIDATE_STATION_lookup_result = SQ_Shortcut_To_RES.select( \
	SQ_Shortcut_To_RES.sys_row_id.alias('sys_row_id'), \
	SQ_Shortcut_To_RES.STA_STN_ID_CHECKOUT.alias('IN_STN_ID')).join(SHORTCUT_TO_LKP_ODY_VALIDATE_STATION_SRC, (col('STN_ID') == col('IN_STN_ID')), 'left')
SHORTCUT_TO_LKP_ODY_VALIDATE_STATION = SHORTCUT_TO_LKP_ODY_VALIDATE_STATION_lookup_result.select( \
	SHORTCUT_TO_LKP_ODY_VALIDATE_STATION_lookup_result.sys_row_id, \
	col('IN_STN_ID'), \
	col('STN_ID') \
)

# COMMAND ----------
# Processing node SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 2


SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON_lookup_result = SQ_Shortcut_To_RES.select( \
	SQ_Shortcut_To_RES.sys_row_id.alias('sys_row_id'), \
	SQ_Shortcut_To_RES.COUPON_2_ID.alias('IN_COUPON_ID')).join(SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON_SRC, (col('COUPON_ID') == col('IN_COUPON_ID')), 'left')
SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON = SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON_lookup_result.select( \
	SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON_lookup_result.sys_row_id, \
	col('IN_COUPON_ID'), \
	col('COUPON_ID') \
)

# COMMAND ----------
# Processing node LKP_CRY_CURR_BRND, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 13

# Joining dataframes LKP_ODY_GET_STN_COUNTRY, Shortcut_to_EXP_RES_SRC to form LKP_CRY_CURR_BRND
LKP_CRY_CURR_BRND_joined = LKP_ODY_GET_STN_COUNTRY.join(Shortcut_to_EXP_RES_SRC, LKP_ODY_GET_STN_COUNTRY.sys_row_id == Shortcut_to_EXP_RES_SRC.sys_row_id, 'inner')

LKP_CRY_CURR_BRND_lookup_result = LKP_CRY_CURR_BRND_joined.select( \
	LKP_ODY_GET_STN_COUNTRY.sys_row_id.alias('sys_row_id'), \
	LKP_ODY_GET_STN_COUNTRY.CRY_ARIMP_CRY_CD.alias('IN_COUNTRY_CD'), \
	Shortcut_to_EXP_RES_SRC.BRN_BRAND_ID.alias('IN_BRN_BRAND_ID')).join(LKP_CRY_CURR_BRND_SRC, (col('CRY_ARIMP_CRY_CD') == col('IN_COUNTRY_CD')) & (col('BRN_BRAND_ID') == col('IN_BRN_BRAND_ID')), 'left')
.withColumn('row_num_CUR_CURR_CD', row_number().over(partitionBy("sys_row_id").orderBy("CUR_CURR_CD")))
LKP_CRY_CURR_BRND = LKP_CRY_CURR_BRND_lookup_result.filter(col("row_num_CUR_CURR_CD") == 1).select( \
	LKP_CRY_CURR_BRND_lookup_result.sys_row_id, \
	col('CUR_CURR_CD') \
)

# COMMAND ----------
# Processing node EXP_ODY_STG_RESERVATIONS, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 98

# Joining dataframes SQ_Shortcut_To_RES, LKP_EXT_PRODUCT_CODE, SHORTCUT_TO_LKP_ODY_VALIDATE_STATION1, SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON2, LKP_RES_CALLER_INFO, SHORTCUT_TO_LKP_ODY_VALIDATE_ACCOUNT, SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON1, SHORTCUT_TO_LKP_ODY_VALIDATE_PRODUCT, Shortcut_to_EXP_RES_SRC, SHORTCUT_TO_LKP_ODY_VALIDATE_STATION, SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON, LKP_CRY_CURR_BRND to form EXP_ODY_STG_RESERVATIONS
EXP_ODY_STG_RESERVATIONS_joined = SQ_Shortcut_To_RES.join(LKP_EXT_PRODUCT_CODE, SQ_Shortcut_To_RES.sys_row_id == LKP_EXT_PRODUCT_CODE.sys_row_id, 'inner') \
 .join(SHORTCUT_TO_LKP_ODY_VALIDATE_STATION1, LKP_EXT_PRODUCT_CODE.sys_row_id == SHORTCUT_TO_LKP_ODY_VALIDATE_STATION1.sys_row_id, 'inner') \
  .join(SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON2, SHORTCUT_TO_LKP_ODY_VALIDATE_STATION1.sys_row_id == SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON2.sys_row_id, 'inner') \
   .join(LKP_RES_CALLER_INFO, SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON2.sys_row_id == LKP_RES_CALLER_INFO.sys_row_id, 'inner') \
    .join(SHORTCUT_TO_LKP_ODY_VALIDATE_ACCOUNT, LKP_RES_CALLER_INFO.sys_row_id == SHORTCUT_TO_LKP_ODY_VALIDATE_ACCOUNT.sys_row_id, 'inner') \
     .join(SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON1, SHORTCUT_TO_LKP_ODY_VALIDATE_ACCOUNT.sys_row_id == SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON1.sys_row_id, 'inner') \
      .join(SHORTCUT_TO_LKP_ODY_VALIDATE_PRODUCT, SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON1.sys_row_id == SHORTCUT_TO_LKP_ODY_VALIDATE_PRODUCT.sys_row_id, 'inner') \
       .join(Shortcut_to_EXP_RES_SRC, SHORTCUT_TO_LKP_ODY_VALIDATE_PRODUCT.sys_row_id == Shortcut_to_EXP_RES_SRC.sys_row_id, 'inner') \
        .join(SHORTCUT_TO_LKP_ODY_VALIDATE_STATION, Shortcut_to_EXP_RES_SRC.sys_row_id == SHORTCUT_TO_LKP_ODY_VALIDATE_STATION.sys_row_id, 'inner') \
         .join(SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON, SHORTCUT_TO_LKP_ODY_VALIDATE_STATION.sys_row_id == SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON.sys_row_id, 'inner') \
          .join(LKP_CRY_CURR_BRND, SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON.sys_row_id == LKP_CRY_CURR_BRND.sys_row_id, 'inner')
.withColumn("V_RES_NBR", ltrim(rtrim(SQ_Shortcut_To_RES.RES_NBR.cast(StringType())))) \
	.withColumn("V_BOOKED_BY_USER", when(SQ_Shortcut_To_RES.CREAT_BY = (lit('GALILEO')),lit('0')).when(SQ_Shortcut_To_RES.CREAT_BY = (lit('WORLDSPAN')),lit('0')).when(SQ_Shortcut_To_RES.CREAT_BY = (lit('AMADEUS')),lit('0')).when(SQ_Shortcut_To_RES.CREAT_BY = (lit('SABRE')),lit('0')).when(SQ_Shortcut_To_RES.CREAT_BY = (lit('WEB')),lit('0')).when(SQ_Shortcut_To_RES.CREAT_BY = (lit('GDS')),lit('0')).when(SQ_Shortcut_To_RES.CREAT_BY = (lit('APOLLO')),lit('0')).when(SQ_Shortcut_To_RES.CREAT_BY = (lit('LNK')),lit('0')).when(SQ_Shortcut_To_RES.CREAT_BY = (col('lit(None)')),lit('0')).otherwise(SQ_Shortcut_To_RES.CREAT_BY)) \
	.withColumn("DISC_AMT", when(SQ_Shortcut_To_RES.DISC_PCT.isNull() , lit(0.0)).otherwise((SQ_Shortcut_To_RES.DISC_PCT * SQ_Shortcut_To_RES.SEAT_PRICE_AMT * lit(.01)))) \
	.withColumn("DAYS", datediff(SQ_Shortcut_To_RES.PROJ_CI_TMSP,SQ_Shortcut_To_RES.PROJ_CO_TMSP)) \
	.withColumn("TURNDOWN_REASON", when(SQ_Shortcut_To_RES.TDD_TRNDN_RSN_CD.isNull() , lit('0')).otherwise(SQ_Shortcut_To_RES.TDD_TRNDN_RSN_CD)) \
	.withColumn("V_INVALID_CO_COMMENT", when(SHORTCUT_TO_LKP_ODY_VALIDATE_STATION.STN_ID == lit('-1') , lit('Invalid CO Station:') + SHORTCUT_TO_LKP_ODY_VALIDATE_STATION.IN_STN_ID).otherwise(col('lit(None)'))) \
	.withColumn("V_INVALID_CI_COMMENT", when(SHORTCUT_TO_LKP_ODY_VALIDATE_STATION1.STN_ID == lit('-1') , lit('Invalid CI Station:') + SHORTCUT_TO_LKP_ODY_VALIDATE_STATION1.IN_STN_ID).otherwise(col('lit(None)'))) \
	.withColumn("V_INVALID_PRODUCT_COMMENT", when(SHORTCUT_TO_LKP_ODY_VALIDATE_PRODUCT.PROD_INST_CD == lit('-1') , lit('Invalid Product:') + SHORTCUT_TO_LKP_ODY_VALIDATE_PRODUCT.IN_PROD_INST_CD).otherwise(col('lit(None)'))) \
	.withColumn("V_INVALID_COUPON_COMMENT", when(SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON1.COUPON_ID == lit('-1') , lit('Invalid Coupon:') + SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON1.IN_COUPON_ID).otherwise(col('lit(None)')) + when(SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON.COUPON_ID == lit('-1') , lit('Invalid Coupon 2') + SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON.IN_COUPON_ID).otherwise(col('lit(None)')) + when(SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON2.COUPON_ID == lit('-1') , lit('Invalid Coupon3:') + SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON2.IN_COUPON_ID).otherwise(col('lit(None)'))) \
	.withColumn("V_CR_CARD", substring(col('CR_CARD_NBR'), 1, 12)) \
	.withColumn("V_CC_NUMBER", col('V_CR_CARD').cast('decimal(12,2)')) \
	.withColumn("V_CH_DAYS", trunc(datediff(SQ_Shortcut_To_RES.PROJ_CI_TMSP,SQ_Shortcut_To_RES.PROJ_CO_TMSP) / lit(1440))) \
	.withColumn("V_CH_MINS", mod(datediff(SQ_Shortcut_To_RES.PROJ_CI_TMSP,SQ_Shortcut_To_RES.PROJ_CO_TMSP) , lit(1440))) \
	.withColumn("V_TOTAL_CH_DAYS", when((col('V_CH_DAYS') > lit(0))  & (col('V_CH_MINS') <= lit(29)) , col('V_CH_DAYS')).otherwise(when((col('V_CH_MINS') >= 30)  & (col('V_CH_MINS') <= 89) , TO_FLOAT(col('V_CH_DAYS') +(lit(1) / lit(3))) , when((col('V_CH_MINS') >= lit(90))  & (col('V_CH_MINS') <= lit(149)) , TO_FLOAT(col('V_CH_DAYS') +(lit(2) / lit(3))) , when(col('V_CH_MINS') >= lit(150) , TO_FLOAT(col('V_CH_DAYS') + lit(1)) , col('V_CH_DAYS')))).cast('decimal(12,2)')))EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS_joined.select( \
	SQ_Shortcut_To_RES.sys_row_id.alias('sys_row_id'), \
	SQ_Shortcut_To_RES.BRN_BRAND_ID.alias('BRN_BRAND_ID'), \
	SQ_Shortcut_To_RES.RES_NBR.alias('RES_NBR'), \
	SQ_Shortcut_To_RES.LST_CHNG_DT.alias('LST_CHNG_DT'), \
	SQ_Shortcut_To_RES.STA_STN_ID_RES_TAKEN.alias('STA_STN_ID_RES_TAKEN'), \
	SQ_Shortcut_To_RES.CANCEL_TMSP.alias('CANCEL_TMSP'), \
	SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON1.IN_COUPON_ID.alias('COUPON_1_ID'), \
	SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON1.COUPON_ID.alias('COUPON_1_VALIDATE'), \
	SQ_Shortcut_To_RES.COUPON_1_QTY.alias('COUPON_1_QTY'), \
	SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON.IN_COUPON_ID.alias('COUPON_2_ID'), \
	SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON.COUPON_ID.alias('COUPON_2_VALIDATE'), \
	SQ_Shortcut_To_RES.COUPON_2_QTY.alias('COUPON_2_QTY'), \
	SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON2.IN_COUPON_ID.alias('COUPON_3_ID'), \
	SHORTCUT_TO_LKP_ODY_VALIDATE_COUPON2.COUPON_ID.alias('COUPON_3_VALIDATE'), \
	SQ_Shortcut_To_RES.COUPON_3_QTY.alias('COUPON_3_QTY'), \
	SQ_Shortcut_To_RES.CREAT_BY.alias('CREAT_BY'), \
	SQ_Shortcut_To_RES.PROJ_CI_TMSP.alias('PROJ_CI_TMSP'), \
	SQ_Shortcut_To_RES.PROJ_CO_TMSP.alias('PROJ_CO_TMSP'), \
	SQ_Shortcut_To_RES.WEB_SRC_CD.alias('WEB_SRC_CD'), \
	SQ_Shortcut_To_RES.RES_SRC_CD.alias('RES_SRC_CD'), \
	SQ_Shortcut_To_RES.RES_STAT.alias('RES_STAT'), \
	SQ_Shortcut_To_RES.RES_TMSP.alias('RES_TMSP'), \
	SHORTCUT_TO_LKP_ODY_VALIDATE_STATION1.STN_ID.alias('STA_STN_ID_CHECKIN'), \
	SHORTCUT_TO_LKP_ODY_VALIDATE_STATION1.IN_STN_ID.alias('STA_STN_ID_CHECKIN_RES_ORIGINAL'), \
	SHORTCUT_TO_LKP_ODY_VALIDATE_STATION.STN_ID.alias('STA_STN_ID_CHECKOUT'), \
	SHORTCUT_TO_LKP_ODY_VALIDATE_STATION.IN_STN_ID.alias('STA_STN_ID_CHECKOUT_RES_ORIGINAL'), \
	SQ_Shortcut_To_RES.VCA_CAT_CD_ACTL.alias('VCA_CAT_CD_ACTL'), \
	SQ_Shortcut_To_RES.CTR_CON_ID_COMM.alias('CTR_CON_ID_COMM'), \
	SHORTCUT_TO_LKP_ODY_VALIDATE_ACCOUNT.IN_CON_ID.alias('CTR_CON_ID_PRIM'), \
	SHORTCUT_TO_LKP_ODY_VALIDATE_ACCOUNT.CON_ID.alias('CTR_CON_ID_PRIM_VALIDATE'), \
	SQ_Shortcut_To_RES.CTR_CON_ID_RENT.alias('CTR_CON_ID_RENT'), \
	SHORTCUT_TO_LKP_ODY_VALIDATE_PRODUCT.PROD_INST_CD.alias('VALIDATE_PROD'), \
	SHORTCUT_TO_LKP_ODY_VALIDATE_PRODUCT.VER_NBR.alias('VALIDATE_VERSN'), \
	SHORTCUT_TO_LKP_ODY_VALIDATE_PRODUCT.IN_PROD_INST_CD.alias('PRC_PROD_INST_CD'), \
	SHORTCUT_TO_LKP_ODY_VALIDATE_PRODUCT.IN_VER_NBR.alias('PRC_VER_NBR'), \
	SQ_Shortcut_To_RES.TDD_TRNDN_RSN_CD.alias('TDD_TRNDN_RSN_CD'), \
	SQ_Shortcut_To_RES.SPCL_EQP_FLG.alias('SPCL_EQP_FLG'), \
	SQ_Shortcut_To_RES.SEAT_PRICE_AMT.alias('SEAT_PRICE_AMT'), \
	SQ_Shortcut_To_RES.DVR_FRST_NM.alias('DVR_FRST_NM'), \
	SQ_Shortcut_To_RES.DVR_SRNM.alias('DVR_SRNM'), \
	SQ_Shortcut_To_RES.DRV_DVR_ID.alias('DRV_DVR_ID'), \
	SQ_Shortcut_To_RES.DISC_PCT.alias('DISC_PCT'), \
	SQ_Shortcut_To_RES.NO_SHOW_AMT.alias('NO_SHOW_AMT'), \
	SQ_Shortcut_To_RES.SLC_SVC_LEV_CD.alias('SLC_SVC_LEV_CD'), \
	SQ_Shortcut_To_RES.FLGT_NBR.alias('FLGT_NBR'), \
	SQ_Shortcut_To_RES.ORG_ID_BKR.alias('ORG_ID_BKR'), \
	SQ_Shortcut_To_RES.ORT_ROLE_TYP_CD_BKR.alias('ORT_ROLE_TYP_CD_BKR'), \
	SQ_Shortcut_To_RES.WRD_WRAP_UP_CD.alias('WRD_WRAP_UP_CD'), \
	LKP_CRY_CURR_BRND.CUR_CURR_CD.alias('CUR_CURR_CD'), \
	SQ_Shortcut_To_RES.DW_COMMENTS.alias('COMMENTS'), \
	SQ_Shortcut_To_RES.FF_FLG.alias('FF_FLG'), \
	SQ_Shortcut_To_RES.PRPY_MIN_AMT.alias('PRPY_MIN_AMT'), \
	SQ_Shortcut_To_RES.CANCEL_AMT.alias('CANCEL_AMT'), \
	SQ_Shortcut_To_RES.MPT_MOP_TYP_CD.alias('MPT_MOP_TYP_CD'), \
	SQ_Shortcut_To_RES.MOP_MOP_CD_PTNTLY.alias('MOP_MOP_CD_PTNTLY'), \
	SQ_Shortcut_To_RES.CRSC_GDS_CD.alias('CRSC_GDS_CD'), \
	SQ_Shortcut_To_RES.GUAR_STAT.alias('GUAR_STAT'), \
	Shortcut_to_EXP_RES_SRC.OUT_DW_RES_SRC_CD.alias('RES_SOURCE'), \
	SQ_Shortcut_To_RES.VCA_CAT_CD_DRVN.alias('VCA_CAT_CD_DRVN'), \
	SQ_Shortcut_To_RES.MARKET_SEGMENT_CD.alias('MARKET_SEGMENT_CD'), \
	SQ_Shortcut_To_RES.EST_STAFF_ID_BEHALF_OF.alias('EST_STAFF_ID_BEHALF_OF'), \
	SQ_Shortcut_To_RES.NCR_NBR.alias('NCR_NBR'), \
	SQ_Shortcut_To_RES.VHCL_GRP_STOPSL_OVRD.alias('VHCL_GRP_STOPSL_OVRD'), \
	SQ_Shortcut_To_RES.PRPY_FLG.alias('PRPY_FLG'), \
	SQ_Shortcut_To_RES.CLR_NAME.alias('CLR_NAME'), \
	LKP_RES_CALLER_INFO.CLR_EMAIL_ADDR.alias('CLR_EMAIL_ADDR'), \
	LKP_RES_CALLER_INFO.AGENT_EMAIL_ADDRESS.alias('AGENT_EMAIL_ADDRESS'), \
	LKP_EXT_PRODUCT_CODE.RES_PEPC_EPROD_ID.alias('RES_PEPC_EPROD_ID'), \
	SQ_Shortcut_To_RES.BAC_ACCT_ID.alias('BAC_ACCT_ID'), \
	SQ_Shortcut_To_RES.E_VOUCHER.alias('E_VOUCHER'), \
	SQ_Shortcut_To_RES.PTP_ELIGIBLE_FLG.alias('PTP_ELIGIBLE_FLG'), \
	SQ_Shortcut_To_RES.PNR_RCD_LOC.alias('PNR_RCD_LOC'), \
	SQ_Shortcut_To_RES.EXTRNL_REF.alias('EXTRNL_REF'), \
	SQ_Shortcut_To_RES.EXTRNL_VCHR_ID.alias('EXTRNL_VCHR_ID'), \
	SQ_Shortcut_To_RES.MEMBERSHIP_ID.alias('MEMBERSHIP_ID'), \
	SQ_Shortcut_To_RES.CTR_CON_ID_PRFR.alias('CTR_CON_ID_PRFR'), \
	SQ_Shortcut_To_RES.NET_REV_AMT.alias('NET_REV_AMT'), \
	SQ_Shortcut_To_RES.POS_COUNTRY_CD.alias('POS_COUNTRY_CD'), \
	SQ_Shortcut_To_RES.CARD_NBR_FSIX.alias('CARD_NBR_FSIX'), \
	SQ_Shortcut_To_RES.CARD_NBR_LFOUR.alias('CARD_NBR_LFOUR'), \
	SQ_Shortcut_To_RES.LOYALTY_TRANS_ID.alias('LOYALTY_TRANS_ID'), \
	SQ_Shortcut_To_RES.COUPON_FIXED_AMT.alias('COUPON_FIXED_AMT')) \
	.withColumn('CR_CARD_NBR', lit(None)) \
	.select( \
	(col('sys_row_id')).alias('sys_row_id'), \
	col('BRN_BRAND_ID'), \
	(lit(1)).alias('SYSTEM_ID'), \
	(TO_CHAR(col('RES_NBR'))).alias('OUT_RES_NUMBER'), \
	col('LST_CHNG_DT'), \
	col('STA_STN_ID_RES_TAKEN'), \
	col('CANCEL_TMSP'), \
	(when(col('COUPON_1_ID').isNull() , lit('0')).otherwise(col('COUPON_1_VALIDATE'))).alias('COUPON_1_OUT'), \
	(when(col('COUPON_1_QTY').isNull() , lit(0)).otherwise(TO_INTEGER(col('COUPON_1_QTY')))).alias('OUT_COUPON_1_QYT'), \
	(when(col('COUPON_2_ID').isNull() , lit('0')).otherwise(col('COUPON_2_VALIDATE'))).alias('COUPON_2_OUT'), \
	(when(col('COUPON_2_QTY').isNull() , lit(0)).otherwise(TO_INTEGER(col('COUPON_2_QTY')))).alias('OUT_COUPON_2_QYT2'), \
	(when(col('COUPON_3_ID').isNull() , lit('0')).otherwise(col('COUPON_3_VALIDATE'))).alias('COUPON_3_OUT'), \
	(when(col('COUPON_3_QTY').isNull() , lit(0)).otherwise(TO_INTEGER(col('COUPON_3_QTY')))).alias('OUT_COUPON_3_QYT'), \
	(when((col('V_BOOKED_BY_USER') == lit('DBI')) OR(col('V_BOOKED_BY_USER') == lit('0')) OR(col('V_BOOKED_BY_USER') == lit('INTNT')) , col('V_BOOKED_BY_USER')).otherwise(when((col('BRN_BRAND_ID') == lit('ET')) AND(SUBSTR(col('V_BOOKED_BY_USER') , 1 , 1) == lit('E')) , col('V_BOOKED_BY_USER') , when(col('BRN_BRAND_ID') == lit('ET') , lit('E') + col('V_BOOKED_BY_USER') , col('V_BOOKED_BY_USER'))))).alias('BOOKED_BY_USER'), \
	col('PROJ_CI_TMSP'), \
	col('PROJ_CO_TMSP'), \
	col('RES_STAT'), \
	col('RES_TMSP'), \
	col('STA_STN_ID_CHECKIN_RES_ORIGINAL'), \
	(when(col('STA_STN_ID_CHECKIN_RES_ORIGINAL').isNull() , lit('0')).otherwise(col('STA_STN_ID_CHECKIN'))).alias('CI_STATION'), \
	col('STA_STN_ID_CHECKOUT_RES_ORIGINAL'), \
	(when(col('STA_STN_ID_CHECKOUT_RES_ORIGINAL').isNull() , lit('0')).otherwise(col('STA_STN_ID_CHECKOUT'))).alias('CO_STATION'), \
	(when(col('CTR_CON_ID_COMM').isNull() , lit('0')).otherwise(TO_CHAR(col('CTR_CON_ID_COMM')))).alias('CTR_ID_COMM'), \
	(when(col('CTR_CON_ID_PRIM').isNull() , lit('0')).otherwise(TO_CHAR(col('CTR_CON_ID_PRIM_VALIDATE')))).alias('CTR_CON_ID_PRIM_STR'), \
	(when(col('CTR_CON_ID_RENT').isNull() , lit('0')).otherwise(TO_CHAR(col('CTR_CON_ID_RENT')))).alias('CTR_CON_ID_RENT_STR'), \
	(when(col('PRC_PROD_INST_CD').isNull() , lit('0')).otherwise(col('VALIDATE_PROD'))).alias('PRC_PROD_INST_CD1'), \
	(when(col('PRC_PROD_INST_CD').isNull() , lit('0')).otherwise(TO_CHAR(col('VALIDATE_VERSN')))).alias('PRODUCT_VSN'), \
	(when(col('SPCL_EQP_FLG') == lit('Y') , lit(1)).otherwise(lit(0))).alias('SPCL_EQ_FLAG'), \
	col('SEAT_PRICE_AMT'), \
	col('DVR_FRST_NM'), \
	col('DVR_SRNM'), \
	(when(col('DRV_DVR_ID').isNull() , lit('0')).otherwise(TO_CHAR(col('DRV_DVR_ID')))).alias('CUSTOMER_DRIVER'), \
	(when(col('DISC_PCT').isNull() , lit(0)).otherwise(TO_INTEGER(col('DISC_PCT')))).alias('DISC_PERCENT'), \
	(col('DISC_AMT')).alias('OUT_DISC_AMT'), \
	(when(col('DISC_PCT').isNull() , col('SEAT_PRICE_AMT')).otherwise(col('SEAT_PRICE_AMT') -(col('SEAT_PRICE_AMT') * col('DISC_PCT') * lit(.01)))).alias('NET_TM'), \
	(when(col('NO_SHOW_AMT').isNull() , lit(0)).otherwise(col('NO_SHOW_AMT'))).alias('OUT_NO_SHOW_AMT'), \
	col('SLC_SVC_LEV_CD'), \
	(when(col('FLGT_NBR').isNull() , lit('0')).otherwise(col('FLGT_NBR'))).alias('AIRLINE_CD'), \
	(when(col('DAYS').isNull() , lit(0.0)).otherwise(col('DAYS'))).alias('DAYS_COUNT'), \
	(lit(1)).alias('RES_COUNT'), \
	(col('SESSSTARTTIME')).alias('LOAD_TS'), \
	(when((col('ORT_ROLE_TYP_CD_BKR') == lit('TA')) AND(col('ORG_ID_BKR').isNotNull()) , lit('TA') + TO_CHAR(col('ORG_ID_BKR'))).otherwise(lit('0'))).alias('TRAVEL_AGENCY_CD'), \
	(when(col('STA_STN_ID_CHECKIN') == col('STA_STN_ID_CHECKOUT') , lit(1)).otherwise(lit(0))).alias('LOCAL_RENT_FLG'), \
	(when(col('STA_STN_ID_CHECKIN') != col('STA_STN_ID_CHECKOUT') , lit(1)).otherwise(lit(0))).alias('ONE_WAY_RENT_FLG'), \
	col('WRD_WRAP_UP_CD'), \
	(when(col('CUR_CURR_CD').isNull() , lit('0')).otherwise(col('CUR_CURR_CD'))).alias('CURRENCY_CD'), \
	col('PRPY_MIN_AMT'), \
	col('CANCEL_AMT'), \
	col('CR_CARD_NBR'), \
	(RPAD(col('CR_CARD_NBR') , 16 , ' ')).alias('CREDIT_CARD_CD'), \
	col('GUAR_STAT'), \
	col('RES_SOURCE'), \
	SQ_Shortcut_To_RES.DW_COMMENTS.alias('OUT_COMMENTS'), \
	(when(col('V_TOTAL_CH_DAYS') < lit(1.0) , lit(1.0000)).otherwise(col('V_TOTAL_CH_DAYS'))).alias('OUT_CHARGE_DAYS'), \
	col('MARKET_SEGMENT_CD'), \
	col('NCR_NBR'), \
	col('VHCL_GRP_STOPSL_OVRD'), \
	col('PRPY_FLG'), \
	col('CLR_NAME'), \
	col('CLR_EMAIL_ADDR'), \
	col('AGENT_EMAIL_ADDRESS'), \
	col('RES_PEPC_EPROD_ID'), \
	(TO_CHAR(col('BAC_ACCT_ID'))).alias('BUS_ACCOUNT_CD'), \
	(when(col('E_VOUCHER').isNull() , lit(0)).otherwise(DECODE(col('E_VOUCHER') , lit('T') , lit(1) , lit('C') , lit(2) , lit(0)))).alias('E_VOUCHER_FLG'), \
	(when(col('PTP_ELIGIBLE_FLG').isNull() , lit(0)).otherwise(DECODE(col('PTP_ELIGIBLE_FLG') , lit('Y') , lit(1) , lit(0)))).alias('PTP_FLG_OUT'), \
	col('PNR_RCD_LOC'), \
	col('EXTRNL_REF'), \
	col('EXTRNL_VCHR_ID'), \
	(TO_CHAR(col('CTR_CON_ID_PRFR'))).alias('PRIM_CONTRACT_CD'), \
	col('NET_REV_AMT'), \
	(decode(true ,(col('NET_REV_AMT').isNull() col('or') col('NET_REV_AMT') == lit(0) col('or') col('NET_REV_AMT') > col('SEAT_PRICE_AMT')) col('and') col('SEAT_PRICE_AMT') != lit(0) col('and') col('SEAT_PRICE_AMT').isNull() == false , col('SEAT_PRICE_AMT') , col('NET_REV_AMT'))).alias('NET_REV_AMT_OUT'), \
	col('POS_COUNTRY_CD'), \
	col('CARD_NBR_FSIX'), \
	col('CARD_NBR_LFOUR'), \
	col('LOYALTY_TRANS_ID'), \
	col('COUPON_FIXED_AMT'), \
	col('V_RES_NBR').alias('V_RES_NBR'), \
	col('V_BOOKED_BY_USER').alias('V_BOOKED_BY_USER'), \
	col('DISC_AMT').alias('DISC_AMT'), \
	col('DAYS').alias('DAYS'), \
	col('TURNDOWN_REASON').alias('TURNDOWN_REASON'), \
	col('V_INVALID_CO_COMMENT').alias('V_INVALID_CO_COMMENT'), \
	col('V_INVALID_CI_COMMENT').alias('V_INVALID_CI_COMMENT'), \
	col('V_INVALID_PRODUCT_COMMENT').alias('V_INVALID_PRODUCT_COMMENT'), \
	col('V_INVALID_COUPON_COMMENT').alias('V_INVALID_COUPON_COMMENT'), \
	col('V_CR_CARD').alias('V_CR_CARD'), \
	col('V_CC_NUMBER').alias('V_CC_NUMBER'), \
	col('V_CH_DAYS').alias('V_CH_DAYS'), \
	col('V_CH_MINS').alias('V_CH_MINS'), \
	col('V_TOTAL_CH_DAYS').alias('V_TOTAL_CH_DAYS') \
)

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column BRAND_ID
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(SHORTCUT_TO_LKP_DIM_BRAND_GDS_SRC, \
	(SHORTCUT_TO_LKP_DIM_BRAND_GDS_SRC.BRAND_GDS_CD == EXP_ODY_STG_RESERVATIONS.BRN_BRAND_ID),'left').select(EXP_ODY_STG_RESERVATIONS['*'], SHORTCUT_TO_LKP_DIM_BRAND_GDS_SRC['BRAND_ID'].alias('ULKP_RETURN_1') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('BRAND_ID',col('ULKP_RETURN_1'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column V_VEH_CLASS_CHD_VAL
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(SHORTCUT_TO_LKP_ODY_VALIDATE_VEH_CLASS_SRC, \
	(SHORTCUT_TO_LKP_ODY_VALIDATE_VEH_CLASS_SRC.CAT_CD == EXP_ODY_STG_RESERVATIONS.VCA_CAT_CD_ACTL),'left').select(EXP_ODY_STG_RESERVATIONS['*'], SHORTCUT_TO_LKP_ODY_VALIDATE_VEH_CLASS_SRC['CAT_CD'].alias('ULKP_RETURN_2') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('V_VEH_CLASS_CHD_VAL',col('ULKP_RETURN_2'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column OUT_VEHICLE_CLASS_RES
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('OUT_VEHICLE_CLASS_RES', (when(col('VCA_CAT_CD_ACTL') == lit('0') , lit('0')).otherwise(when(col('V_VEH_CLASS_CHD_VAL').isNull() , lit('-1') , col('VCA_CAT_CD_ACTL')))).alias('OUT_VEHICLE_CLASS_RES'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column VALIDATE_TDD_TRNDN_RSN_CD
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(LKP_VALIDATE_DW_DIM_TURNDOWN_SRC, \
	(LKP_VALIDATE_DW_DIM_TURNDOWN_SRC.TURNDOWN_CD == EXP_ODY_STG_RESERVATIONS.TDD_TRNDN_RSN_CD),'left').select(EXP_ODY_STG_RESERVATIONS['*'], LKP_VALIDATE_DW_DIM_TURNDOWN_SRC['TURNDOWN_ID'].alias('ULKP_RETURN_3') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('VALIDATE_TDD_TRNDN_RSN_CD',col('ULKP_RETURN_3'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column WRAP_UP_TYPE
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(LKP_WRAP_UP_TYPE_SRC, \
	(LKP_WRAP_UP_TYPE_SRC.WRAP_UP_CD == EXP_ODY_STG_RESERVATIONS.WRD_WRAP_UP_CD),'left').select(EXP_ODY_STG_RESERVATIONS['*'], LKP_WRAP_UP_TYPE_SRC['RMS_WRAP_UP_TYP'].alias('ULKP_RETURN_4') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('WRAP_UP_TYPE',col('ULKP_RETURN_4'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column OUT_TD_REASON
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('OUT_TD_REASON', (when((col('RES_STAT') == lit('W')) AND(col('VALIDATE_TDD_TRNDN_RSN_CD').isNull()) , col('WRD_WRAP_UP_CD')).otherwise(when((col('RES_STAT') == lit('W')) AND(col('WRAP_UP_TYPE') == lit('T')) , col('TURNDOWN_REASON') , when((col('RES_STAT') == lit('W')) AND(col('WRD_WRAP_UP_CD').isNull()) , lit('0') , when(col('RES_STAT') == lit('W') , col('WRD_WRAP_UP_CD') , when(col('RES_STAT') == lit('TD') , col('TURNDOWN_REASON') , lit('0'))))))).alias('OUT_TD_REASON'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column V_LOOKUP_MOP_VAL
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(SHORTCUT_TO_LKP_ODY_VALIDATE_MOP_SRC, \
	(SHORTCUT_TO_LKP_ODY_VALIDATE_MOP_SRC.MOP_CD == EXP_ODY_STG_RESERVATIONS.MOP_MOP_CD_PTNTLY),'left').select(EXP_ODY_STG_RESERVATIONS['*'], SHORTCUT_TO_LKP_ODY_VALIDATE_MOP_SRC['MOP_CD'].alias('ULKP_RETURN_5') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('V_LOOKUP_MOP_VAL',col('ULKP_RETURN_5'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column V_LOOKUP_MOP_MOP_VAL
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(SHORTCUT_TO_LKP_ODY_VALIDATE_MOP_MOP_SRC, \
	(SHORTCUT_TO_LKP_ODY_VALIDATE_MOP_MOP_SRC.MOP_TYP_CD == EXP_ODY_STG_RESERVATIONS.MPT_MOP_TYP_CD),'left').select(EXP_ODY_STG_RESERVATIONS['*'], SHORTCUT_TO_LKP_ODY_VALIDATE_MOP_MOP_SRC['MOP_TYP_CD'].alias('ULKP_RETURN_6') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('V_LOOKUP_MOP_MOP_VAL',col('ULKP_RETURN_6'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column OUT_RES_MOP_CD
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('OUT_RES_MOP_CD', (when((col('MPT_MOP_TYP_CD') == lit('0')) AND(col('MOP_MOP_CD_PTNTLY') == lit('0')) , lit('0')).otherwise(when((col('V_LOOKUP_MOP_MOP_VAL').isNotNull()) AND(col('V_LOOKUP_MOP_VAL').isNotNull()) , col('MOP_MOP_CD_PTNTLY') , when((col('V_LOOKUP_MOP_MOP_VAL').isNotNull()) AND(col('V_LOOKUP_MOP_VAL').isNull()) , col('MPT_MOP_TYP_CD') , lit('-1'))))).alias('OUT_RES_MOP_CD'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column V_FREQ_FLYER_CD
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(LKP_DWH_STG_RES_FF_SRC, \
	(LKP_DWH_STG_RES_FF_SRC.RES_DOC_CD == EXP_ODY_STG_RESERVATIONS.V_RES_NBR),'left').select(EXP_ODY_STG_RESERVATIONS['*'], LKP_DWH_STG_RES_FF_SRC['FREQ_FLYER_CD'].alias('ULKP_RETURN_7') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('V_FREQ_FLYER_CD',col('ULKP_RETURN_7'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column V_FREQ_FLYER_CD_TD
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(LKP_FF_RRAS_PGM_ID_TERADATA_SRC, \
	(LKP_FF_RRAS_PGM_ID_TERADATA_SRC.res_res_nbr == EXP_ODY_STG_RESERVATIONS.RES_NBR),'left').select(EXP_ODY_STG_RESERVATIONS['*'], LKP_FF_RRAS_PGM_ID_TERADATA_SRC['tpr_pgm_id'].alias('ULKP_RETURN_8') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('V_FREQ_FLYER_CD_TD',col('ULKP_RETURN_8'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column FREQ_FLYER_CD
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('FREQ_FLYER_CD', (when(col('V_FREQ_FLYER_CD').isNotNull() , col('V_FREQ_FLYER_CD')).otherwise(when(col('V_FREQ_FLYER_CD_TD').isNotNull() , col('V_FREQ_FLYER_CD_TD') , lit('0')))).alias('FREQ_FLYER_CD'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column SPCL_EQUIP_CD1
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(LKP_SPECIAL_EQUIPMENT_SRC, \
	(LKP_SPECIAL_EQUIPMENT_SRC.RES_NUMBER == EXP_ODY_STG_RESERVATIONS.RES_NBR) & (LKP_SPECIAL_EQUIPMENT_SRC.POSITION == lit(1)),'left').select(EXP_ODY_STG_RESERVATIONS['*'], LKP_SPECIAL_EQUIPMENT_SRC['SPCL_EQUIP_CD'].alias('ULKP_RETURN_9') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('SPCL_EQUIP_CD1',col('ULKP_RETURN_9'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column SPCL_EQUIP_QTY1
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(LKP_SPECIAL_EQUIP_QTY_SRC, \
	(LKP_SPECIAL_EQUIP_QTY_SRC.RES_NUMBER == EXP_ODY_STG_RESERVATIONS.RES_NBR) & (LKP_SPECIAL_EQUIP_QTY_SRC.POSITION == lit(1)),'left').select(EXP_ODY_STG_RESERVATIONS['*'], LKP_SPECIAL_EQUIP_QTY_SRC['SPCL_EQUIP_QTY'].alias('ULKP_RETURN_10') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('SPCL_EQUIP_QTY1',col('ULKP_RETURN_10'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column SPCL_EQUIP_VALID1
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(LKP_SPECIAL_EQUIP_VALID_SRC, \
	(LKP_SPECIAL_EQUIP_VALID_SRC.RES_NUMBER == EXP_ODY_STG_RESERVATIONS.RES_NBR) & (LKP_SPECIAL_EQUIP_VALID_SRC.POSITION == lit(1)),'left').select(EXP_ODY_STG_RESERVATIONS['*'], LKP_SPECIAL_EQUIP_VALID_SRC['VALID_EQUIP_CD'].alias('ULKP_RETURN_11') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('SPCL_EQUIP_VALID1',col('ULKP_RETURN_11'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column OUT_SPCL_EQUIP_CD1
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('OUT_SPCL_EQUIP_CD1', (when(col('SPCL_EQUIP_VALID1') == lit(1) , col('SPCL_EQUIP_CD1')).otherwise(lit('0'))).alias('OUT_SPCL_EQUIP_CD1'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column OUT_SPCL_EQUIP_QTY1
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('OUT_SPCL_EQUIP_QTY1', (when(col('SPCL_EQUIP_VALID1') == lit(1) , col('SPCL_EQUIP_QTY1')).otherwise(TO_DECIMAL('0'))).alias('OUT_SPCL_EQUIP_QTY1'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column SPCL_COMMENT
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('SPCL_COMMENT', when(col('SPCL_EQUIP_VALID1') == lit(0) , lit('Invalid SPCL Equip: ') + col('SPCL_EQUIP_CD1')))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column SPCL_EQUIP_CD2
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(LKP_SPECIAL_EQUIPMENT_SRC, \
	(LKP_SPECIAL_EQUIPMENT_SRC.RES_NUMBER == EXP_ODY_STG_RESERVATIONS.RES_NBR) & (LKP_SPECIAL_EQUIPMENT_SRC.POSITION == lit(2)),'left').select(EXP_ODY_STG_RESERVATIONS['*'], LKP_SPECIAL_EQUIPMENT_SRC['SPCL_EQUIP_CD'].alias('ULKP_RETURN_12') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('SPCL_EQUIP_CD2',col('ULKP_RETURN_12'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column SPCL_EQUIP_QTY2
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(LKP_SPECIAL_EQUIP_QTY_SRC, \
	(LKP_SPECIAL_EQUIP_QTY_SRC.RES_NUMBER == EXP_ODY_STG_RESERVATIONS.RES_NBR) & (LKP_SPECIAL_EQUIP_QTY_SRC.POSITION == lit(2)),'left').select(EXP_ODY_STG_RESERVATIONS['*'], LKP_SPECIAL_EQUIP_QTY_SRC['SPCL_EQUIP_QTY'].alias('ULKP_RETURN_13') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('SPCL_EQUIP_QTY2',col('ULKP_RETURN_13'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column SPCL_EQUIP_VALID2
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(LKP_SPECIAL_EQUIP_VALID_SRC, \
	(LKP_SPECIAL_EQUIP_VALID_SRC.RES_NUMBER == EXP_ODY_STG_RESERVATIONS.RES_NBR) & (LKP_SPECIAL_EQUIP_VALID_SRC.POSITION == lit(2)),'left').select(EXP_ODY_STG_RESERVATIONS['*'], LKP_SPECIAL_EQUIP_VALID_SRC['VALID_EQUIP_CD'].alias('ULKP_RETURN_14') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('SPCL_EQUIP_VALID2',col('ULKP_RETURN_14'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column SPCL_COMMENT2
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('SPCL_COMMENT2', when(col('SPCL_EQUIP_VALID2') == lit(0) , lit('Invalid SPCL Equip: ') + col('SPCL_EQUIP_CD2')))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column SPCL_EQUIP_CD3
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(LKP_SPECIAL_EQUIPMENT_SRC, \
	(LKP_SPECIAL_EQUIPMENT_SRC.RES_NUMBER == EXP_ODY_STG_RESERVATIONS.RES_NBR) & (LKP_SPECIAL_EQUIPMENT_SRC.POSITION == lit(3)),'left').select(EXP_ODY_STG_RESERVATIONS['*'], LKP_SPECIAL_EQUIPMENT_SRC['SPCL_EQUIP_CD'].alias('ULKP_RETURN_15') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('SPCL_EQUIP_CD3',col('ULKP_RETURN_15'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column SPCL_EQUIP_QTY3
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(LKP_SPECIAL_EQUIP_QTY_SRC, \
	(LKP_SPECIAL_EQUIP_QTY_SRC.RES_NUMBER == EXP_ODY_STG_RESERVATIONS.RES_NBR) & (LKP_SPECIAL_EQUIP_QTY_SRC.POSITION == lit(3)),'left').select(EXP_ODY_STG_RESERVATIONS['*'], LKP_SPECIAL_EQUIP_QTY_SRC['SPCL_EQUIP_QTY'].alias('ULKP_RETURN_16') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('SPCL_EQUIP_QTY3',col('ULKP_RETURN_16'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column SPCL_EQUIP_VALID3
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(LKP_SPECIAL_EQUIP_VALID_SRC, \
	(LKP_SPECIAL_EQUIP_VALID_SRC.RES_NUMBER == EXP_ODY_STG_RESERVATIONS.RES_NBR) & (LKP_SPECIAL_EQUIP_VALID_SRC.POSITION == lit(3)),'left').select(EXP_ODY_STG_RESERVATIONS['*'], LKP_SPECIAL_EQUIP_VALID_SRC['VALID_EQUIP_CD'].alias('ULKP_RETURN_17') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('SPCL_EQUIP_VALID3',col('ULKP_RETURN_17'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column SPCL_COMMENT3
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('SPCL_COMMENT3', when(col('SPCL_EQUIP_VALID3') == lit(0) , lit('Invalid SPCL Equip: ') + col('SPCL_EQUIP_CD3')))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column SPCL_EQUIP_CD4
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(LKP_SPECIAL_EQUIPMENT_SRC, \
	(LKP_SPECIAL_EQUIPMENT_SRC.RES_NUMBER == EXP_ODY_STG_RESERVATIONS.RES_NBR) & (LKP_SPECIAL_EQUIPMENT_SRC.POSITION == lit(4)),'left').select(EXP_ODY_STG_RESERVATIONS['*'], LKP_SPECIAL_EQUIPMENT_SRC['SPCL_EQUIP_CD'].alias('ULKP_RETURN_18') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('SPCL_EQUIP_CD4',col('ULKP_RETURN_18'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column SPCL_EQUIP_QTY4
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(LKP_SPECIAL_EQUIP_QTY_SRC, \
	(LKP_SPECIAL_EQUIP_QTY_SRC.RES_NUMBER == EXP_ODY_STG_RESERVATIONS.RES_NBR) & (LKP_SPECIAL_EQUIP_QTY_SRC.POSITION == lit(4)),'left').select(EXP_ODY_STG_RESERVATIONS['*'], LKP_SPECIAL_EQUIP_QTY_SRC['SPCL_EQUIP_QTY'].alias('ULKP_RETURN_19') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('SPCL_EQUIP_QTY4',col('ULKP_RETURN_19'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column SPCL_EQUIP_VALID4
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(LKP_SPECIAL_EQUIP_VALID_SRC, \
	(LKP_SPECIAL_EQUIP_VALID_SRC.RES_NUMBER == EXP_ODY_STG_RESERVATIONS.RES_NBR) & (LKP_SPECIAL_EQUIP_VALID_SRC.POSITION == lit(4)),'left').select(EXP_ODY_STG_RESERVATIONS['*'], LKP_SPECIAL_EQUIP_VALID_SRC['VALID_EQUIP_CD'].alias('ULKP_RETURN_20') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('SPCL_EQUIP_VALID4',col('ULKP_RETURN_20'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column SPCL_COMMENT4
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('SPCL_COMMENT4', when(col('SPCL_EQUIP_VALID4') == lit(0) , lit('Invalid SPCL Equip: ') + col('SPCL_EQUIP_CD4')))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column SPCL_EQUIP_CD5
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(LKP_SPECIAL_EQUIPMENT_SRC, \
	(LKP_SPECIAL_EQUIPMENT_SRC.RES_NUMBER == EXP_ODY_STG_RESERVATIONS.RES_NBR) & (LKP_SPECIAL_EQUIPMENT_SRC.POSITION == lit(5)),'left').select(EXP_ODY_STG_RESERVATIONS['*'], LKP_SPECIAL_EQUIPMENT_SRC['SPCL_EQUIP_CD'].alias('ULKP_RETURN_21') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('SPCL_EQUIP_CD5',col('ULKP_RETURN_21'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column SPCL_EQUIP_QTY5
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(LKP_SPECIAL_EQUIP_QTY_SRC, \
	(LKP_SPECIAL_EQUIP_QTY_SRC.RES_NUMBER == EXP_ODY_STG_RESERVATIONS.RES_NBR) & (LKP_SPECIAL_EQUIP_QTY_SRC.POSITION == lit(5)),'left').select(EXP_ODY_STG_RESERVATIONS['*'], LKP_SPECIAL_EQUIP_QTY_SRC['SPCL_EQUIP_QTY'].alias('ULKP_RETURN_22') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('SPCL_EQUIP_QTY5',col('ULKP_RETURN_22'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column SPCL_EQUIP_VALID5
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(LKP_SPECIAL_EQUIP_VALID_SRC, \
	(LKP_SPECIAL_EQUIP_VALID_SRC.RES_NUMBER == EXP_ODY_STG_RESERVATIONS.RES_NBR) & (LKP_SPECIAL_EQUIP_VALID_SRC.POSITION == lit(5)),'left').select(EXP_ODY_STG_RESERVATIONS['*'], LKP_SPECIAL_EQUIP_VALID_SRC['VALID_EQUIP_CD'].alias('ULKP_RETURN_23') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('SPCL_EQUIP_VALID5',col('ULKP_RETURN_23'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column SPCL_COMMENT5
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('SPCL_COMMENT5', when(col('SPCL_EQUIP_VALID5') == lit(0) , lit('Invalid SPCL Equip: ') + col('SPCL_EQUIP_CD5')))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column SPCL_EQUIP_CD6
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(LKP_SPECIAL_EQUIPMENT_SRC, \
	(LKP_SPECIAL_EQUIPMENT_SRC.RES_NUMBER == EXP_ODY_STG_RESERVATIONS.RES_NBR) & (LKP_SPECIAL_EQUIPMENT_SRC.POSITION == lit(6)),'left').select(EXP_ODY_STG_RESERVATIONS['*'], LKP_SPECIAL_EQUIPMENT_SRC['SPCL_EQUIP_CD'].alias('ULKP_RETURN_24') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('SPCL_EQUIP_CD6',col('ULKP_RETURN_24'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column SPCL_EQUIP_QTY6
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(LKP_SPECIAL_EQUIP_QTY_SRC, \
	(LKP_SPECIAL_EQUIP_QTY_SRC.RES_NUMBER == EXP_ODY_STG_RESERVATIONS.RES_NBR) & (LKP_SPECIAL_EQUIP_QTY_SRC.POSITION == lit(6)),'left').select(EXP_ODY_STG_RESERVATIONS['*'], LKP_SPECIAL_EQUIP_QTY_SRC['SPCL_EQUIP_QTY'].alias('ULKP_RETURN_25') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('SPCL_EQUIP_QTY6',col('ULKP_RETURN_25'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column SPCL_EQUIP_VALID6
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(LKP_SPECIAL_EQUIP_VALID_SRC, \
	(LKP_SPECIAL_EQUIP_VALID_SRC.RES_NUMBER == EXP_ODY_STG_RESERVATIONS.RES_NBR) & (LKP_SPECIAL_EQUIP_VALID_SRC.POSITION == lit(6)),'left').select(EXP_ODY_STG_RESERVATIONS['*'], LKP_SPECIAL_EQUIP_VALID_SRC['VALID_EQUIP_CD'].alias('ULKP_RETURN_26') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('SPCL_EQUIP_VALID6',col('ULKP_RETURN_26'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column SPCL_COMMENT6
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('SPCL_COMMENT6', when(col('SPCL_EQUIP_VALID6') == lit(0) , lit('Invalid SPCL Equip: ') + col('SPCL_EQUIP_CD6')))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column SPCL_EQUIP_CD7
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(LKP_SPECIAL_EQUIPMENT_SRC, \
	(LKP_SPECIAL_EQUIPMENT_SRC.RES_NUMBER == EXP_ODY_STG_RESERVATIONS.RES_NBR) & (LKP_SPECIAL_EQUIPMENT_SRC.POSITION == lit(7)),'left').select(EXP_ODY_STG_RESERVATIONS['*'], LKP_SPECIAL_EQUIPMENT_SRC['SPCL_EQUIP_CD'].alias('ULKP_RETURN_27') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('SPCL_EQUIP_CD7',col('ULKP_RETURN_27'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column SPCL_EQUIP_QTY7
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(LKP_SPECIAL_EQUIP_QTY_SRC, \
	(LKP_SPECIAL_EQUIP_QTY_SRC.RES_NUMBER == EXP_ODY_STG_RESERVATIONS.RES_NBR) & (LKP_SPECIAL_EQUIP_QTY_SRC.POSITION == lit(7)),'left').select(EXP_ODY_STG_RESERVATIONS['*'], LKP_SPECIAL_EQUIP_QTY_SRC['SPCL_EQUIP_QTY'].alias('ULKP_RETURN_28') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('SPCL_EQUIP_QTY7',col('ULKP_RETURN_28'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column SPCL_EQUIP_VALID7
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(LKP_SPECIAL_EQUIP_VALID_SRC, \
	(LKP_SPECIAL_EQUIP_VALID_SRC.RES_NUMBER == EXP_ODY_STG_RESERVATIONS.RES_NBR) & (LKP_SPECIAL_EQUIP_VALID_SRC.POSITION == lit(7)),'left').select(EXP_ODY_STG_RESERVATIONS['*'], LKP_SPECIAL_EQUIP_VALID_SRC['VALID_EQUIP_CD'].alias('ULKP_RETURN_29') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('SPCL_EQUIP_VALID7',col('ULKP_RETURN_29'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column SPCL_COMMENT7
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('SPCL_COMMENT7', when(col('SPCL_EQUIP_VALID7') == lit(0) , lit('Invalid SPCL Equip: ') + col('SPCL_EQUIP_CD7')))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column OUT_SPCL_EQUIP_CD2
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('OUT_SPCL_EQUIP_CD2', (when(col('SPCL_EQUIP_VALID2') == lit(1) , col('SPCL_EQUIP_CD2')).otherwise(lit('0'))).alias('OUT_SPCL_EQUIP_CD2'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column OUT_SPCL_EQUIP_QTY2
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('OUT_SPCL_EQUIP_QTY2', (when(col('SPCL_EQUIP_VALID2') == lit(1) , col('SPCL_EQUIP_QTY2')).otherwise(TO_DECIMAL('0'))).alias('OUT_SPCL_EQUIP_QTY2'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column OUT_SPCL_EQUIP_CD3
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('OUT_SPCL_EQUIP_CD3', (when(col('SPCL_EQUIP_VALID3') == lit(1) , col('SPCL_EQUIP_CD3')).otherwise(lit('0'))).alias('OUT_SPCL_EQUIP_CD3'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column OUT_SPCL_EQUIP_QTY3
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('OUT_SPCL_EQUIP_QTY3', (when(col('SPCL_EQUIP_VALID3') == lit(1) , col('SPCL_EQUIP_QTY3')).otherwise(TO_DECIMAL('0'))).alias('OUT_SPCL_EQUIP_QTY3'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column OUT_SPCL_EQUIP_CD4
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('OUT_SPCL_EQUIP_CD4', (when(col('SPCL_EQUIP_VALID4') == lit(1) , col('SPCL_EQUIP_CD4')).otherwise(lit('0'))).alias('OUT_SPCL_EQUIP_CD4'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column OUT_SPCL_EQUIP_QTY4
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('OUT_SPCL_EQUIP_QTY4', (when(col('SPCL_EQUIP_VALID4') == lit(1) , col('SPCL_EQUIP_QTY4')).otherwise(TO_DECIMAL('0'))).alias('OUT_SPCL_EQUIP_QTY4'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column OUT_SPCL_EQUIP_CD5
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('OUT_SPCL_EQUIP_CD5', (when(col('SPCL_EQUIP_VALID5') == lit(1) , col('SPCL_EQUIP_CD5')).otherwise(lit('0'))).alias('OUT_SPCL_EQUIP_CD5'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column OUT_SPCL_EQUIP_QTY5
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('OUT_SPCL_EQUIP_QTY5', (when(col('SPCL_EQUIP_VALID5') == lit(1) , col('SPCL_EQUIP_QTY5')).otherwise(TO_DECIMAL('0'))).alias('OUT_SPCL_EQUIP_QTY5'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column OUT_SPCL_EQUIP_CD6
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('OUT_SPCL_EQUIP_CD6', (when(col('SPCL_EQUIP_VALID6') == lit(1) , col('SPCL_EQUIP_CD6')).otherwise(lit('0'))).alias('OUT_SPCL_EQUIP_CD6'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column OUT_SPCL_EQUIP_QTY6
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('OUT_SPCL_EQUIP_QTY6', (when(col('SPCL_EQUIP_VALID6') == lit(1) , col('SPCL_EQUIP_QTY6')).otherwise(TO_DECIMAL('0'))).alias('OUT_SPCL_EQUIP_QTY6'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column OUT_SPCL_EQUIP_CD7
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('OUT_SPCL_EQUIP_CD7', (when(col('SPCL_EQUIP_VALID7') == lit(1) , col('SPCL_EQUIP_CD7')).otherwise(lit('0'))).alias('OUT_SPCL_EQUIP_CD7'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column OUT_SPCL_EQUIP_QTY7
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('OUT_SPCL_EQUIP_QTY7', (when(col('SPCL_EQUIP_VALID7') == lit(1) , col('SPCL_EQUIP_QTY7')).otherwise(TO_DECIMAL('0'))).alias('OUT_SPCL_EQUIP_QTY7'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column DEBIT_CARD_FLG
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(SHORTCUT_TO_LKP_INT_DEBIT_CARD_BINS_SRC, \
	(SHORTCUT_TO_LKP_INT_DEBIT_CARD_BINS_SRC.BIN_LO_NUMBER >= EXP_ODY_STG_RESERVATIONS.V_CC_NUMBER) & (SHORTCUT_TO_LKP_INT_DEBIT_CARD_BINS_SRC.BIN_HI_NUMBER <= EXP_ODY_STG_RESERVATIONS.V_CC_NUMBER),'left').select(EXP_ODY_STG_RESERVATIONS['*'], SHORTCUT_TO_LKP_INT_DEBIT_CARD_BINS_SRC['BIN_HI_NUMBER'].alias('ULKP_RETURN_30') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('DEBIT_CARD_FLG',when(col('ULKP_RETURN_30').isNull() , lit(0)).otherwise(lit(1)))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column V_VEHICLE_CLASS_RES_VAL
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(SHORTCUT_TO_LKP_ODY_VALIDATE_VEH_CLASS_SRC, \
	(SHORTCUT_TO_LKP_ODY_VALIDATE_VEH_CLASS_SRC.CAT_CD == EXP_ODY_STG_RESERVATIONS.VCA_CAT_CD_DRVN),'left').select(EXP_ODY_STG_RESERVATIONS['*'], SHORTCUT_TO_LKP_ODY_VALIDATE_VEH_CLASS_SRC['CAT_CD'].alias('ULKP_RETURN_31') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('V_VEHICLE_CLASS_RES_VAL',col('ULKP_RETURN_31'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column OUT_VEHICLE_CLASS_CHARGED
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('OUT_VEHICLE_CLASS_CHARGED', (when(col('VCA_CAT_CD_DRVN') == lit('0') , lit('0')).otherwise(when(col('V_VEHICLE_CLASS_RES_VAL').isNull() , lit('-1') , col('VCA_CAT_CD_DRVN')))).alias('OUT_VEHICLE_CLASS_CHARGED'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column BEHALF_OF_USER_CD
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(SHORTCUT_TO_LKP_ODY_EC_STAFF_SRC, \
	(SHORTCUT_TO_LKP_ODY_EC_STAFF_SRC.STAFF_ID == EXP_ODY_STG_RESERVATIONS.EST_STAFF_ID_BEHALF_OF),'left').select(EXP_ODY_STG_RESERVATIONS['*'], SHORTCUT_TO_LKP_ODY_EC_STAFF_SRC['UNIX_USR'].alias('ULKP_RETURN_32') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('BEHALF_OF_USER_CD',col('ULKP_RETURN_32'))

# Adding deferred logic for dataframe EXP_ODY_STG_RESERVATIONS, column MEMBERSHIP_ID_FF_RRAS
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.join(LKP_ODY_FF_RRAS_TD_SRC, \
	(LKP_ODY_FF_RRAS_TD_SRC.res_res_nbr == EXP_ODY_STG_RESERVATIONS.RES_NBR),'left').select(EXP_ODY_STG_RESERVATIONS['*'], LKP_ODY_FF_RRAS_TD_SRC['mbr_nbr'].alias('ULKP_RETURN_33') )
EXP_ODY_STG_RESERVATIONS = EXP_ODY_STG_RESERVATIONS.withColumn('MEMBERSHIP_ID_FF_RRAS',col('ULKP_RETURN_33'))

# COMMAND ----------
# Processing node LKP_SPCL_INST, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4


LKP_SPCL_INST_lookup_result = EXP_ODY_STG_RESERVATIONS.select( \
	EXP_ODY_STG_RESERVATIONS.sys_row_id.alias('sys_row_id'), \
	EXP_ODY_STG_RESERVATIONS.OUT_RES_NUMBER.alias('IN_RES_DOC_CD')).join(LKP_SPCL_INST_SRC, (col('RES_DOC_CD') == col('IN_RES_DOC_CD')), 'left')
LKP_SPCL_INST = LKP_SPCL_INST_lookup_result.select( \
	LKP_SPCL_INST_lookup_result.sys_row_id, \
	col('SPCL_INSTRS'), \
	col('CASH_IN_CLUB_NBR') \
)

# COMMAND ----------
# Processing node Shortcut_To_EXP_1_3RD_CHARGE_DAYS, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 1

.withColumn("V_CH_DAYS", trunc(datediff(EXP_ODY_STG_RESERVATIONS.PROJ_CI_TMSP,EXP_ODY_STG_RESERVATIONS.PROJ_CO_TMSP) / lit(1440))) \
	.withColumn("V_CH_MINS", mod(datediff(EXP_ODY_STG_RESERVATIONS.PROJ_CI_TMSP,EXP_ODY_STG_RESERVATIONS.PROJ_CO_TMSP) , lit(1440))) \
	.withColumn("V_TOTAL_CH_DAYS", when((col('V_CH_DAYS') >= lit(0))  & (col('V_CH_MINS') <= lit(29)) , col('V_CH_DAYS')).otherwise(when((col('V_CH_MINS') >= 30)  & (col('V_CH_MINS') <= 89) , TO_FLOAT(col('V_CH_DAYS') +(lit(1) / lit(3))) , when((col('V_CH_MINS') >= lit(90))  & (col('V_CH_MINS') <= lit(149)) , TO_FLOAT(col('V_CH_DAYS') +(lit(2) / lit(3))) , when(col('V_CH_MINS') >= lit(150) , TO_FLOAT(col('V_CH_DAYS') + lit(1)) , col('V_CH_DAYS')))).cast('decimal(12,2)')))Shortcut_To_EXP_1_3RD_CHARGE_DAYS = EXP_ODY_STG_RESERVATIONS.select( \
	EXP_ODY_STG_RESERVATIONS.sys_row_id.alias('sys_row_id'), \
	EXP_ODY_STG_RESERVATIONS.PROJ_CO_TMSP.alias('CO_DATE'), \
	EXP_ODY_STG_RESERVATIONS.PROJ_CI_TMSP.alias('CI_DATE')).select( \
	(col('sys_row_id')).alias('sys_row_id'), \
	(when(col('V_TOTAL_CH_DAYS') < lit(1.0) , lit(1.0000)).otherwise(col('V_TOTAL_CH_DAYS'))).alias('OUT_CHARGE_DAYS') \
)

# COMMAND ----------
# Processing node Shortcut_To_STG_RESERVATIONS, type TARGET 
# COLUMN COUNT: 103

# Joining dataframes SQ_Shortcut_To_RES, EXP_ODY_STG_RESERVATIONS, LKP_SPCL_INST, Shortcut_To_EXP_1_3RD_CHARGE_DAYS to form Shortcut_To_STG_RESERVATIONS
Shortcut_To_STG_RESERVATIONS_joined = SQ_Shortcut_To_RES.join(EXP_ODY_STG_RESERVATIONS, SQ_Shortcut_To_RES.sys_row_id == EXP_ODY_STG_RESERVATIONS.sys_row_id, 'inner') \
 .join(LKP_SPCL_INST, EXP_ODY_STG_RESERVATIONS.sys_row_id == LKP_SPCL_INST.sys_row_id, 'inner') \
  .join(Shortcut_To_EXP_1_3RD_CHARGE_DAYS, LKP_SPCL_INST.sys_row_id == Shortcut_To_EXP_1_3RD_CHARGE_DAYS.sys_row_id, 'inner')

Shortcut_To_STG_RESERVATIONS = Shortcut_To_STG_RESERVATIONS_joined.select( \
	EXP_ODY_STG_RESERVATIONS.BRAND_IDcol("BRAND_ID").alias('BRAND_ID'), \
	EXP_ODY_STG_RESERVATIONS.SYSTEM_IDcol("SYSTEM_ID").alias('SYSTEM_ID'), \
	EXP_ODY_STG_RESERVATIONS.OUT_RES_NUMBERcol("RES_DOC_CD").alias('RES_DOC_CD'), \
	EXP_ODY_STG_RESERVATIONS.RES_TMSP.cast(DateType()).alias('DATE_BOOK'), \
	EXP_ODY_STG_RESERVATIONS.PROJ_CO_TMSP.cast(DateType()).alias('DATE_PROJ_CO'), \
	EXP_ODY_STG_RESERVATIONS.PROJ_CI_TMSP.cast(DateType()).alias('DATE_PROJ_CI'), \
	EXP_ODY_STG_RESERVATIONS.CANCEL_TMSP.cast(DateType()).alias('DATE_CANCEL'), \
	EXP_ODY_STG_RESERVATIONS.LST_CHNG_DT.cast(DateType()).alias('DATE_LAST_MDFY'), \
	EXP_ODY_STG_RESERVATIONS.CI_STATIONcol("RES_STATION_CI").alias('RES_STATION_CI'), \
	EXP_ODY_STG_RESERVATIONS.CO_STATIONcol("RES_STATION_CO").alias('RES_STATION_CO'), \
	EXP_ODY_STG_RESERVATIONS.STA_STN_ID_RES_TAKENcol("RES_STATION_TAKEN").alias('RES_STATION_TAKEN'), \
	EXP_ODY_STG_RESERVATIONS.CTR_ID_COMMcol("CONTRACT1_ID").alias('CONTRACT1_ID'), \
	EXP_ODY_STG_RESERVATIONS.CTR_CON_ID_PRIM_STRcol("CONTRACT2_ID").alias('CONTRACT2_ID'), \
	EXP_ODY_STG_RESERVATIONS.CTR_CON_ID_RENT_STRcol("CONTRACT3_ID").alias('CONTRACT3_ID'), \
	EXP_ODY_STG_RESERVATIONS.SLC_SVC_LEV_CDcol("SERVICE_LEVEL").alias('SERVICE_LEVEL'), \
	EXP_ODY_STG_RESERVATIONS.PRC_PROD_INST_CD1col("PRODUCT").alias('PRODUCT'), \
	EXP_ODY_STG_RESERVATIONS.PRODUCT_VSNcol("PRODUCT_VSN").alias('PRODUCT_VSN'), \
	EXP_ODY_STG_RESERVATIONS.OUT_VEHICLE_CLASS_REScol("VEHICLE_CLASS_RES").alias('VEHICLE_CLASS_RES'), \
	EXP_ODY_STG_RESERVATIONS.RES_STATcol("RES_STATUS").alias('RES_STATUS'), \
	EXP_ODY_STG_RESERVATIONS.RES_SOURCEcol("RES_SOURCE").alias('RES_SOURCE'), \
	EXP_ODY_STG_RESERVATIONS.OUT_TD_REASONcol("TURNDOWN_REASON").alias('TURNDOWN_REASON'), \
	EXP_ODY_STG_RESERVATIONS.TRAVEL_AGENCY_CDcol("TRAVEL_AGENCY").alias('TRAVEL_AGENCY'), \
	EXP_ODY_STG_RESERVATIONS.BOOKED_BY_USERcol("BOOKED_BY_USER").alias('BOOKED_BY_USER'), \
	EXP_ODY_STG_RESERVATIONS.COUPON_1_OUTcol("COUPON1_ID").alias('COUPON1_ID'), \
	EXP_ODY_STG_RESERVATIONS.COUPON_2_OUTcol("COUPON2_ID").alias('COUPON2_ID'), \
	EXP_ODY_STG_RESERVATIONS.COUPON_3_OUTcol("COUPON3_ID").alias('COUPON3_ID'), \
	EXP_ODY_STG_RESERVATIONS.AIRLINE_CDcol("AIRLINE").alias('AIRLINE'), \
	EXP_ODY_STG_RESERVATIONS.CUSTOMER_DRIVERcol("CUSTOMER_DRIVER").alias('CUSTOMER_DRIVER'), \
	EXP_ODY_STG_RESERVATIONS.DVR_FRST_NMcol("CUST_FIRST_NAME").alias('CUST_FIRST_NAME'), \
	EXP_ODY_STG_RESERVATIONS.DVR_SRNMcol("CUST_LAST_NAME").alias('CUST_LAST_NAME'), \
	EXP_ODY_STG_RESERVATIONS.CURRENCY_CDcol("CURRENCY").alias('CURRENCY'), \
	EXP_ODY_STG_RESERVATIONS.SPCL_EQ_FLAGcol("SPECIAL_EQ_FLG").alias('SPECIAL_EQ_FLG'), \
	EXP_ODY_STG_RESERVATIONS.LOCAL_RENT_FLGcol("LOCAL_RENT_FLG").alias('LOCAL_RENT_FLG'), \
	EXP_ODY_STG_RESERVATIONS.ONE_WAY_RENT_FLGcol("ONE_WAY_RENT_FLG").alias('ONE_WAY_RENT_FLG'), \
	EXP_ODY_STG_RESERVATIONS.GUAR_STATcol("GUARANTEED_FLG").alias('GUARANTEED_FLG'), \
	EXP_ODY_STG_RESERVATIONS.SEAT_PRICE_AMTcol("SEAT_PRICE_AMT").alias('SEAT_PRICE_AMT'), \
	EXP_ODY_STG_RESERVATIONS.DISC_PERCENTcol("DISCOUNT_PCT").alias('DISCOUNT_PCT'), \
	EXP_ODY_STG_RESERVATIONS.OUT_DISC_AMTcol("DISCOUNT_AMT").alias('DISCOUNT_AMT'), \
	EXP_ODY_STG_RESERVATIONS.NET_TMcol("NET_AMT").alias('NET_AMT'), \
	EXP_ODY_STG_RESERVATIONS.OUT_NO_SHOW_AMTcol("NO_SHOW_AMT").alias('NO_SHOW_AMT'), \
	EXP_ODY_STG_RESERVATIONS.OUT_COUPON_1_QYTcol("COUPON1_COUNT").alias('COUPON1_COUNT'), \
	EXP_ODY_STG_RESERVATIONS.OUT_COUPON_2_QYT2col("COUPON2_COUNT").alias('COUPON2_COUNT'), \
	EXP_ODY_STG_RESERVATIONS.OUT_COUPON_3_QYTcol("COUPON3_COUNT").alias('COUPON3_COUNT'), \
	EXP_ODY_STG_RESERVATIONS.RES_COUNTcol("RES_COUNT").alias('RES_COUNT'), \
	EXP_ODY_STG_RESERVATIONS.DAYS_COUNTcol("DAYS_COUNT").alias('DAYS_COUNT'), \
	lit(None)col("SUNDAY_COUNT").alias('SUNDAY_COUNT'), \
	lit(None)col("MONDAY_COUNT").alias('MONDAY_COUNT'), \
	lit(None)col("TUESDAY_COUNT").alias('TUESDAY_COUNT'), \
	lit(None)col("WEDNESDAY_COUNT").alias('WEDNESDAY_COUNT'), \
	lit(None)col("THURSDAY_COUNT").alias('THURSDAY_COUNT'), \
	lit(None)col("FRIDAY_COUNT").alias('FRIDAY_COUNT'), \
	lit(None)col("SATURDAY_COUNT").alias('SATURDAY_COUNT'), \
	EXP_ODY_STG_RESERVATIONS.LOAD_TS.cast(DateType()).alias('LOAD_TS'), \
	EXP_ODY_STG_RESERVATIONS.OUT_COMMENTScol("RES_COMMENTS").alias('RES_COMMENTS'), \
	EXP_ODY_STG_RESERVATIONS.FREQ_FLYER_CDcol("FREQ_FLYER_CD").alias('FREQ_FLYER_CD'), \
	EXP_ODY_STG_RESERVATIONS.OUT_RES_MOP_CDcol("RES_MOP_CD").alias('RES_MOP_CD'), \
	EXP_ODY_STG_RESERVATIONS.CANCEL_AMTcol("CANCEL_AMT").alias('CANCEL_AMT'), \
	EXP_ODY_STG_RESERVATIONS.PRPY_MIN_AMTcol("PRE_PAY_AMT").alias('PRE_PAY_AMT'), \
	EXP_ODY_STG_RESERVATIONS.OUT_SPCL_EQUIP_CD1col("SPCL_EQUIP_CD1").alias('SPCL_EQUIP_CD1'), \
	EXP_ODY_STG_RESERVATIONS.OUT_SPCL_EQUIP_QTY1col("SPCL_EQUIP_QTY1").alias('SPCL_EQUIP_QTY1'), \
	EXP_ODY_STG_RESERVATIONS.OUT_SPCL_EQUIP_CD2col("SPCL_EQUIP_CD2").alias('SPCL_EQUIP_CD2'), \
	EXP_ODY_STG_RESERVATIONS.OUT_SPCL_EQUIP_QTY2col("SPCL_EQUIP_QTY2").alias('SPCL_EQUIP_QTY2'), \
	EXP_ODY_STG_RESERVATIONS.OUT_SPCL_EQUIP_CD3col("SPCL_EQUIP_CD3").alias('SPCL_EQUIP_CD3'), \
	EXP_ODY_STG_RESERVATIONS.OUT_SPCL_EQUIP_QTY3col("SPCL_EQUIP_QTY3").alias('SPCL_EQUIP_QTY3'), \
	EXP_ODY_STG_RESERVATIONS.OUT_SPCL_EQUIP_CD4col("SPCL_EQUIP_CD4").alias('SPCL_EQUIP_CD4'), \
	EXP_ODY_STG_RESERVATIONS.OUT_SPCL_EQUIP_QTY4col("SPCL_EQUIP_QTY4").alias('SPCL_EQUIP_QTY4'), \
	EXP_ODY_STG_RESERVATIONS.OUT_SPCL_EQUIP_CD5col("SPCL_EQUIP_CD5").alias('SPCL_EQUIP_CD5'), \
	EXP_ODY_STG_RESERVATIONS.OUT_SPCL_EQUIP_QTY5col("SPCL_EQUIP_QTY5").alias('SPCL_EQUIP_QTY5'), \
	EXP_ODY_STG_RESERVATIONS.OUT_SPCL_EQUIP_CD6col("SPCL_EQUIP_CD6").alias('SPCL_EQUIP_CD6'), \
	EXP_ODY_STG_RESERVATIONS.OUT_SPCL_EQUIP_QTY6col("SPCL_EQUIP_QTY6").alias('SPCL_EQUIP_QTY6'), \
	EXP_ODY_STG_RESERVATIONS.OUT_SPCL_EQUIP_CD7col("SPCL_EQUIP_CD7").alias('SPCL_EQUIP_CD7'), \
	EXP_ODY_STG_RESERVATIONS.OUT_SPCL_EQUIP_QTY7col("SPCL_EQUIP_QTY7").alias('SPCL_EQUIP_QTY7'), \
	EXP_ODY_STG_RESERVATIONS.DEBIT_CARD_FLGcol("DEBIT_FLG").alias('DEBIT_FLG'), \
	EXP_ODY_STG_RESERVATIONS.OUT_VEHICLE_CLASS_CHARGEDcol("VEH_CLASS_CHARGED").alias('VEH_CLASS_CHARGED'), \
	Shortcut_To_EXP_1_3RD_CHARGE_DAYS.OUT_CHARGE_DAYScol("CHARGE_DAYS").alias('CHARGE_DAYS'), \
	LKP_SPCL_INST.SPCL_INSTRScol("SPCL_INSTRS").alias('SPCL_INSTRS'), \
	EXP_ODY_STG_RESERVATIONS.MARKET_SEGMENT_CDcol("SEGMENT_CD").alias('SEGMENT_CD'), \
	lit(None)col("CHANNEL_ID").alias('CHANNEL_ID'), \
	LKP_SPCL_INST.CASH_IN_CLUB_NBRcol("CASH_IN_CLUB_NBR").alias('CASH_IN_CLUB_NBR'), \
	EXP_ODY_STG_RESERVATIONS.BEHALF_OF_USER_CDcol("BEHALF_OF_USER_CD").alias('BEHALF_OF_USER_CD'), \
	EXP_ODY_STG_RESERVATIONS.NCR_NBRcol("EURO_RESNUM").alias('EURO_RESNUM'), \
	EXP_ODY_STG_RESERVATIONS.RES_PEPC_EPROD_IDcol("EXT_PRODUCT_CD").alias('EXT_PRODUCT_CD'), \
	EXP_ODY_STG_RESERVATIONS.VHCL_GRP_STOPSL_OVRDcol("OVERSELL_FLG").alias('OVERSELL_FLG'), \
	EXP_ODY_STG_RESERVATIONS.CLR_EMAIL_ADDRcol("CALLER_EMAIL").alias('CALLER_EMAIL'), \
	EXP_ODY_STG_RESERVATIONS.AGENT_EMAIL_ADDRESScol("AGENT_EMAIL").alias('AGENT_EMAIL'), \
	EXP_ODY_STG_RESERVATIONS.PRPY_FLGcol("PREPAY_FLG").alias('PREPAY_FLG'), \
	EXP_ODY_STG_RESERVATIONS.CLR_NAMEcol("CALLER_NAME").alias('CALLER_NAME'), \
	EXP_ODY_STG_RESERVATIONS.BUS_ACCOUNT_CDcol("BUS_ACCOUNT_CD").alias('BUS_ACCOUNT_CD'), \
	EXP_ODY_STG_RESERVATIONS.E_VOUCHER_FLGcol("E_VOUCHER_FLG").alias('E_VOUCHER_FLG'), \
	EXP_ODY_STG_RESERVATIONS.PTP_FLG_OUTcol("PTP_ELIGIBLE_FLG").alias('PTP_ELIGIBLE_FLG'), \
	EXP_ODY_STG_RESERVATIONS.PNR_RCD_LOCcol("RECORD_LOCATOR").alias('RECORD_LOCATOR'), \
	EXP_ODY_STG_RESERVATIONS.EXTRNL_REFcol("EXTERNAL_REFERENCE_CD").alias('EXTERNAL_REFERENCE_CD'), \
	EXP_ODY_STG_RESERVATIONS.EXTRNL_VCHR_IDcol("VOUCHER_CD").alias('VOUCHER_CD'), \
	SQ_Shortcut_To_RES.PROD_CURR_SEAT_PRICEcol("PROD_CURR_SEAT_PRICE").alias('PROD_CURR_SEAT_PRICE'), \
	SQ_Shortcut_To_RES.PROD_CURR_CDcol("PROD_CURR_CD").alias('PROD_CURR_CD'), \
	EXP_ODY_STG_RESERVATIONS.MEMBERSHIP_ID_FF_RRAScol("MEMBERSHIP_CD").alias('MEMBERSHIP_CD'), \
	EXP_ODY_STG_RESERVATIONS.PRIM_CONTRACT_CDcol("PRIM_CONTRACT_CD").alias('PRIM_CONTRACT_CD'), \
	EXP_ODY_STG_RESERVATIONS.NET_REV_AMT_OUTcol("NET_REVENUE_AMT").alias('NET_REVENUE_AMT'), \
	EXP_ODY_STG_RESERVATIONS.CARD_NBR_FSIXcol("PCARD_FIRST_6_TXT").alias('PCARD_FIRST_6_TXT'), \
	EXP_ODY_STG_RESERVATIONS.CARD_NBR_LFOURcol("PCARD_LAST_4_TXT").alias('PCARD_LAST_4_TXT'), \
	EXP_ODY_STG_RESERVATIONS.POS_COUNTRY_CDcol("POS_COUNTRY_CD").alias('POS_COUNTRY_CD'), \
	EXP_ODY_STG_RESERVATIONS.LOYALTY_TRANS_IDcol("LOYALTY_TRANS_ID").alias('LOYALTY_TRANS_ID'), \
	EXP_ODY_STG_RESERVATIONS.COUPON_FIXED_AMTcol("COUPON_FIXED_AMT").alias('COUPON_FIXED_AMT') \
)
Shortcut_To_STG_RESERVATIONS.write.saveAsTable('OSDWADMIN.STG_RESERVATIONS', mode = 'append')

quit()