#!/bin/ksh

###############################################################################
# PROGRAM: ES_HOMECITY_L3_REFRESH.SH 
# 
# DESCRIPTION: THIS PROCESS WILL DO THE TABLE SWAPPING AND COLLECT STATS
#       STEPS: 1: RENAME TARGET TABLE TO OLD TABLE 
#              2: RENAME NEW TABLE TO TARGET TABLE
#              3: RENAME OLD TABLE TO NEW TABLE
#              4: DELETE NEW TABLE DATA
#
###############################################################################


BTEQ_SCRIPT=`basename $0 | cut -f1 -d.`
LOGFILE=$DW_LDIR/"${BTEQ_SCRIPT}_sh_"`date +"%Y%m%d%H%M%S"`.log

bteq << !EOF! > ${LOGFILE} 2>${LOGFILE}
  .RUN FILE=${TLOGON};
  .SET ERROROUT STDOUT;
  .SET WIDTH 500;
  .SET TITLEDASHES OFF;
  .EXPORT RESET;
  
/* RENAME TARGET TABLE TO OLD TABLE */

  RENAME TABLE ECARS2_RPT_TB.HOME_CITY_EMPL_CNTRCT_SALES 
            TO ECARS2_RPT_TB.HOME_CITY_EMPL_CNTRCT_SALES_O;

  .IF ERRORCODE <> 0 THEN .EXIT ERRORCODE	

/* RENAME NEW TABLE TO TARGET TABLE */

  RENAME TABLE ECARS2_RPT_TB.HOME_CITY_EMPL_CNTRCT_SALES_N
            TO ECARS2_RPT_TB.HOME_CITY_EMPL_CNTRCT_SALES;

  .IF ERRORCODE <> 0 THEN .EXIT ERRORCODE	

/* RENAME OLD TABLE TO NEW TABLE */

  RENAME TABLE ECARS2_RPT_TB.HOME_CITY_EMPL_CNTRCT_SALES_O
            TO ECARS2_RPT_TB.HOME_CITY_EMPL_CNTRCT_SALES_N;

  .IF ERRORCODE <> 0 THEN .EXIT ERRORCODE	

/* DELETE DATA FROM NEW TABLE */
       
   DELETE FROM ECARS2_RPT_TB.HOME_CITY_EMPL_CNTRCT_SALES_N;

  .IF ERRORCODE <> 0 THEN .EXIT ERRORCODE   

   COLLECT STATS ON ECARS2_RPT_TB.HOME_CITY_EMPL_CNTRCT_SALES; 

  .IF ERRORCODE <> 0 THEN .EXIT ERRORCODE    
 		
  .EXIT 0
!EOF!

bteq_rc=$?

#
# Check the return code and display an appropriate message to the console.
#
if [[ ${bteq_rc} != 0 ]] then
	     echo "$APPL_NAM"
	     echo
	     echo "RUN STATUS:  ["FAILED"]" 
       echo "LOG FILE:    ["${LOGFILE}"]"
	     echo "BTEQ END:    ["`date`"]"
	     exit 1;
else
  echo "$APPL_NAM"
  echo
  echo "RUN STATUS: ["SUCCESS"]"
  echo "BTEQ END:   ["`date`"]"
fi	

	


echo "LOG FILE:     ["${LOGFILE}"]"
return ${bteq_rc}
#
# End of Shell script.
#

exit 0