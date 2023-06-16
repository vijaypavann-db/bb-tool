#!/bin/ksh

###############################################################################
# PROGRAM: ES_L3_HOMECITY_SETDATEPARMS.SH
# 
# OVERVIEW:  THE PROCESS WILL RUN TWICE DAILY FOR NA AND EU.
#            PROCESS WILL READ THE APPL_CNTRL_DATA TABLE AND INSERT THE START 
#            AND END DATE INTO THE ES_DATEPARMS TABLE
#            
#    STEPS:   1: CLEAR ECARS2_RPT_WT.ES_DATEPARMS TABLE DURING EVERY RUN.
#             2: INSERT START AND END DATE RECS INTO ECARS2_RPT_WT.ES_DATEPARMS
#                FROM THE ETL_TB.APPL_CNTRL_DATA.
#                
#                START DATE: GET THE LATEST COMPLETED RECORD FOR THE CNTRY CODE
#                            AND SYS_NAM
#                END DATE: GET THE INPROCESS RECORD BY CNTRY CODE AND SYS_NAM
#             3: COLLECT STATS ON ECARS2_RPT_WT.ES_DATEPARMS
#
################################################################################


##############
# UNIX SETUP #
##############

BTEQ_SCRIPT=`basename $0 | cut -f1 -d.`
LOGFILE=$DW_LDIR/"${BTEQ_SCRIPT}_sh_"`date +"%Y%m%d%H%M%S"`.log

if [[ $# -ne 1 ]] 
then 
          echo "$APPL_NAM_ES"
          echo
          echo 'Incorrect number of arguments specified from TWS. Usage: program NA/EU'
          echo "RUN STATUS:  ["ERROR"]"
          echo "BTEQ END:    ["`date`"]"

   exit 1;
fi

CNTRY_CDE=$1
NEW_APPL_NAM_ES="${APPL_NAM_ES} $CNTRY_CDE"

# Display a start time.

  echo "BTEQ Start Date/time:  ["`date`"]"


##################
#   BTEQ CODE    #
##################

bteq << !EOF! > ${LOGFILE} 2>${LOGFILE}
  .RUN FILE=${TLOGON};
  .SET ERROROUT STDOUT;
  .SET WIDTH 500;
  .SET TITLEDASHES OFF;
  .EXPORT RESET;

/* STEP 1: DELETE PARM TABLE */


DELETE FROM ECARS2_RPT_WT.ES_DATEPARMS;

  .IF ERRORCODE <> 0 THEN .EXIT ERRORCODE 

/* STEP 2: INSERT START AND END DATE RECS INTO ECARS2_RPT_WT.ES_DATEPARMS */
        
/* GET THE GREATEST COMPLETE RECORD PER COUNTRY CODE AS START DATE AND */
/* GET THE INPROCESS RECORD AS END DATE */

  INSERT ECARS2_RPT_WT.ES_DATEPARMS  
  SELECT cast(max(b.data_as_of_tsp) AS date FORMAT 'YYYY-MM-DD') AS START_CST_DTE
       , cast(a.data_as_of_tsp AS date FORMAT 'YYYY-MM-DD') AS END_CST_DTE
    FROM ETL.APPL_CNTRL_DATA A
   INNER JOIN
         ETL.APPL_CNTRL_DATA B
      ON TRIM(A.sys_nam) = TRIM(b.sys_nam)
     AND TRIM(a.appl_stat_dsc) = TRIM('INPROCESS')
     AND TRIM(b.appl_stat_dsc) = TRIM('COMPLETE')
     AND TRIM(A.appl_Nam) = TRIM('${NEW_APPL_NAM_ES}')
     AND TRIM(B.appl_Nam) = TRIM('${NEW_APPL_NAM_ES}')
   GROUP BY a.data_as_of_tsp ;
      
  .IF ERRORCODE <> 0 THEN .EXIT ERRORCODE 
  .IF ACTIVITYCOUNT = 0 THEN .QUIT 98

/* STEP 3: COLLECT STATS ON ECARS2_RPT_WT */ 

  COLLECT STATISTICS ON ECARS2_RPT_WT.ES_DATEPARMS;  

  .IF ERRORCODE <> 0 THEN .EXIT ERRORCODE 
      
  .LOGOFF
  .QUIT ERRORCODE

/* EXIT WITH ZERO RETURN CODE IF ALL GOES WELL. */

  .EXIT 0
!EOF!

################
# UNIX WRAP UP #
################

bteq_rc=$?

# CHECK THE RETURN CODE #

if [[ ${bteq_rc} != 0 ]]
then
   if [ ${bteq_rc} = 98 ] ; then
       echo 
       echo "$NEW_APPL_NAM_ES COULD NOT CREATE DATE PARAMETERS"
       echo
       echo "RUN STATUS:  ["ABORTED"]"      
       echo "LOG FILE:    ["${LOGFILE}"]"
       echo "BTEQ END:    ["`date`"]"
       exit 98
    else 
       echo "$NEW_APPL_NAM_ES" 
       echo 
       echo "RUN STATUS:  ["FAILED"]" 
       echo "RETURN CODE: ["${bteq_rc}"]"
       echo "LOG FILE:    ["${LOGFILE}"]"
       echo "BTEQ END:    ["`date`"]"
       exit 1
    fi
else
  echo "$NEW_APPL_NAM_ES"
  echo
  echo "RUN STATUS: ["SUCCESS"]"
  echo "BTEQ END:   ["`date`"]"
fi  

echo "LOG FILE:     ["${LOGFILE}"]"
return ${bteq_rc}

# EXIT SHELL #

exit 0




