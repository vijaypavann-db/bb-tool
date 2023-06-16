#!/bin/ksh

#############################################################################
# PROGRAM: ES_L3_HOMECITY_INIT.SH
# 
# OVERVIEW:  THE PROCESS WILL RUN TWICE DAILY FOR NA AND EU.
#            A COUNTRY CODE PARAMETER WILL BE SENT TO THE PROGRAM TO IDENTIFY 
#            WHICH COUNTRY CODE TO PROCESS. 
#            
#    STEPS:   1: CHECK TO SEE IF WE HAVE A RECORD INPROCESS. 
#                THE PROCESS WILL ABORT IF SO.
#             2: INSERT A 'INPROCESS' RECORD INTO ETL_TB.APPL_CNTRL_DATA.
#                THIS RECORD WILL BE MARKED WITH COUNTRY CODE IN THE TABLE AND
#                BE USED AS THE END DATE.
#
##############################################################################


##############
# UNIX SETUP #
##############

BTEQ_SCRIPT=`basename $0 | cut -f1 -d.`
LOGFILE=$DW_LDIR/"${BTEQ_SCRIPT}_sh_"`date +"%Y%m%d%H%M%S"`.log

# CHECK THE NUMBER OF ARGUMENTS

if [[ $# -ne 1 ]] 
then 
          echo "$APPL_NAM_ES"
          echo
          echo 'Incorrect number of arguments specified from TWS. Usage: program NA/EU'
          echo "RUN STATUS:   ["ERROR"]"
          echo "BTEQ END:     ["`date`"]"

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

/* STEP 1: CHECK THE PROCESS FOR A RECORD INPROCESS */

  SELECT DATA_AS_OF_TSP 
    FROM ETL.APPL_CNTRL_DATA
   WHERE TRIM(SYS_NAM) = TRIM('${SYS_NAM_ES}')
     AND TRIM(APPL_NAM) = TRIM('${NEW_APPL_NAM_ES}')
     AND TRIM(APPL_STAT_DSC) = TRIM('INPROCESS') ;
  
  .IF ACTIVITYCOUNT > 0 THEN .QUIT 99
     
/* STEP 2: INSERT A CURRENT RECORD AS INPROCESS INTO CONTROL TABLE */

 INSERT INTO ETL_TB.APPL_CNTRL_DATA  
     (
       sys_nam
      , appl_nam
      , proc_strm_strt_tsp
      , proc_strm_end_tsp
      , data_as_of_tsp
      , appl_stat_dsc
      , dw_crte_tsp
     )
VALUES( TRIM('${SYS_NAM_ES}')      --sys_nam
     , TRIM('${NEW_APPL_NAM_ES}') --appl_nam
     , current_timestamp(6)       --proc_strm_strt_tsp
     , NULL                       --proc_strm_end_tsp
     , CASE WHEN (TRIM('${CNTRY_CDE}') = 'EU') 
        THEN CAST(CAST(SYSLIB.ERAC_CT_TO_GMT(CAST(CURRENT_TIMESTAMP AS TIMESTAMP)) AS DATE) AS TIMESTAMP)
        ELSE CAST(CURRENT_date AS timestamp) 
        END    --data_as_of_tsp
     , 'INPROCESS'                --appl_stat_dsc
     , current_timestamp(6)       --dw_crte_tsp
     );
            
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
   if [ ${bteq_rc} = 99 ] ; then
       echo 
       echo "$NEW_APPL_NAM_ES HAS IDENTIFIED A RECORD INPROCESS. UNABLE TO PROCESS."
       echo
       echo "RUN STATUS:  ["ABORTED"]"
       echo "LOG FILE:    ["${LOGFILE}"]"
       echo "BTEQ END:    ["`date`"]"
       exit 99
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




