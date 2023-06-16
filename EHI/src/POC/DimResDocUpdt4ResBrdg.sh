#!/bin/ksh
. $DW_DIR/HndShkTrgFn.sh
#################################################################################################
###
### This script updates the erac_rent_cntrct_nbr column in Dim_Res_Document for
### ECARS2 tickets with Odyssey reservations.It was created to address the following CQ
### CQ-165564 IRDW: FACT_RENTAL_PROFIT.res_doc_id not set for ECARS2 ticket with ODY reservation
###
#################################################################################################

PGM=`basename $0 | cut -f1 -d.`
LOGFILE=$DW_LDIR/"${PGM}_sh_"`date +"%Y%m%d%H%M%S"`.log

# Remove HandshaKe Trigger file -- Call function that'll try & wait if trigger file is absent DW-1737
Rm_HndshkTrg $ODY_HNDSHK/dim_res_document.trg  $LOGFILE
if [ "$?" -ne 0 ] ; then   
  echo "${PGM} process failed, unable to remove Handshake trigger ! Check logfile $LOGFILE" | tee -a "$LOGFILE"  
  exit 1
fi

bteq << !@! >> $LOGFILE 2>> $LOGFILE
    .run file=$TLOGON;
    .set errorout stdout;

--   Create a table to hold the ecars2 tickets with Ody Res -scre_chnl_cde = 1
--   Join with erac_dateparms to pickup tickets added since last run.

      CREATE VOLATILE TABLE vt_brdg_tkt AS (
      SELECT RC.rent_cntrct_nbr, RC.gui_resv_nbr
      FROM   ECARS2.RENT_CNTRCT RC
      CROSS JOIN   INTGRT_RPT_WT.ERAC_DATEPARMS Z
      WHERE  RC.srce_chnl_cde = 1     -- Ecars2 tickets with Ody Reservation
      AND    RC.rent_cntrct_stat_cde IN (5,6,7,10,11)  -- Limit to reservations
      AND    RC.eff_strt_tsp >= Z.START_CST_TSP
      AND    RC.eff_strt_tsp < Z.END_CST_TSP GROUP BY 1,2
      ) WITH DATA ON COMMIT PRESERVE ROWS;

      .IF ERRORCODE <> 0 THEN .QUIT ERRORCODE

-- Join the above table with Dim_res to get pertinent information to update in next step

      CREATE VOLATILE TABLE vt_res AS (
      SELECT MAX(b.rent_cntrct_nbr) as  res_rent_cntrct_nbr, r.reservation_cd, r.res_id
      FROM   INTGRT_RPT.DIM_RES_DOCUMENT r
      JOIN   vt_brdg_tkt b
      ON     b.gui_resv_nbr = r.reservation_cd
      AND    r.system_id = 1
--      AND    r.brand_id = 21  -- CQ 186892
      AND    r.erac_rent_cntrct_nbr IS NULL
      GROUP BY 2,3                                    -- CQ-167531
      ) WITH DATA ON COMMIT PRESERVE ROWS;

      .IF ERRORCODE <> 0 THEN .QUIT ERRORCODE

      UPDATE  DRSD
      FROM
          INTGRT_RPT_TB.DIM_RES_DOCUMENT   AS  DRSD ,
          vt_res
      SET
              erac_rent_cntrct_nbr            = vt_res.res_rent_cntrct_nbr
      ,       load_ts                         = CURRENT_TIMESTAMP(0)
      WHERE DRSD.res_id = vt_res.res_id
      AND   DRSD.reservation_cd = vt_res.reservation_cd
      ;

      .IF ERRORCODE <> 0 THEN .QUIT ERRORCODE

       .logoff
       .quit ERRORCODE
!@!

rc=$?

if [ "$rc" -ne 0 ] ; then
    touch $ODY_HNDSHK/dim_res_document.trg 
    if [ "$?" -ne 0 ] ; then   
      echo "${PGM} process failed, unable to touch Handshake trigger ! Check logfile $LOGFILE" | tee -a "$LOGFILE"  
      exit 1
    fi
    echo "Rent_cntrct_nbr update of DIM_RES_DOC records failed for ECARS2 Tickets with ODY Reservation."
    exit 1
  else
    touch $ODY_HNDSHK/dim_res_document.trg 
    if [ "$?" -ne 0 ] ; then   
      echo "${PGM} process failed, unable to touch Handshake trigger ! Check logfile $LOGFILE" | tee -a "$LOGFILE"  
      exit 1
    fi  
    echo " Rent_cntrct_nbr update of DIM_RES_DOC records succesful for ECARS2 Tickets with ODY Reservation " | tee -a "$LOGFILE"
    echo "See log file $LOGFILE"  | tee -a "$LOGFILE"
    exit 0
fi
