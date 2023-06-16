#!/bin/ksh

PGM=`basename $0 | cut -f1 -d.`
LOGFILE=$DW_LDIR/"${PGM}_sh_"`date +"%Y%m%d%H%M%S"`.log

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
ES_HC_COMPLETE_TRIGGER=$DW_LDIR/es_homecity_l3_complete_${CNTRY_CDE}.trg


bteq << !@! > $LOGFILE 2> $LOGFILE
    .run file=$TLOGON;
    .set errorout stdout;

    --Close IR RPT process Ctrl
    UPDATE ETL_TB.APPL_CNTRL_DATA
     FROM (
        SELECT sys_nam, appl_nam, proc_strm_strt_tsp
        FROM ETL_TB.APPL_CNTRL_DATA
        WHERE TRIM(appl_nam) = TRIM('${NEW_APPL_NAM_ES}')
        AND TRIM(sys_nam) = TRIM('${SYS_NAM_ES}')
        AND appl_stat_dsc IN ( 'INPROCESS', 'FAILED' )
        QUALIFY row_number() OVER ( PARTITION BY sys_nam, appl_nam ORDER BY proc_strm_strt_tsp DESC)
 = 1 ) DT
    SET appl_stat_dsc = 'COMPLETE', proc_strm_end_tsp = current_timestamp
    WHERE ETL_TB.APPL_CNTRL_DATA.appl_nam = DT.appl_nam
    AND ETL_TB.APPL_CNTRL_DATA.sys_nam = DT.sys_nam
    AND ETL_TB.APPL_CNTRL_DATA.proc_strm_strt_tsp = DT.proc_strm_strt_tsp;

    .if activitycount = 0 then .quit 99
    .quit errorcode


!@!

# capture the return code
bteq_rc=$?


find $DW_LDIR -name "*" -type f -mtime +5 -exec rm -f {} \;


if [[ $bteq_rc !=  0 ]]
then
 #send the error to console
       echo "$NEW_APPL_NAM_ES: UPDATE FAILED" 
       echo
       echo "RUN STATUS:  ["FAILED"]"  
       echo "RETURN CODE: ["${bteq_rc}"]"
       echo "LOG FILE:    ["${LOGFILE}"]"
       echo "BTEQ END:    ["`date`"]"
       exit 1
else
  echo "$NEW_APPL_NAM_ES"
  echo
  echo "RUN STATUS:    ["SUCCESS"]"
  echo "BTEQ END:      ["`date`"]"
  touch $ES_HC_COMPLETE_TRIGGER
  exit 0
fi




