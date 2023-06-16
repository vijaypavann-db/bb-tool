#!/bin/ksh

#/**************************************************************************
#
# Script Name :  upd_dim_res_doc.sh 
#
# Details of script  :  Update TD with ODY data

# AUTHOR :   Kevin Farmer
#
# Initial: November 2022
#
#***************************************************************************/

PGM=`basename $0 | cut -f1 -d.`

LOGFILE=$DW_LDIR/"${PGM}_sh_"`date +"%Y%m%d%H%M%S"`.log

# MAIN PROGRAM 
bteq <<!@! >>"$LOGFILE" 2>> "$LOGFILE"
	.run file=$TLOGON
	.set errorout stdout

  /******************************************************/
  /* UPDATE TABLE DIM_RES_DOCUMENT */
  /******************************************************/
  

MERGE INTO
              INTGRT_RPT_TB.DIM_RES_DOCUMENT    AS  DIM_RES
USING         INTGRT_RPT_WT.DIM_RES_DOC_COPY_TO_TD  AS  PRELOAD   
ON 
   (DIM_RES.RES_ID = PRELOAD.RES_ID)

WHEN MATCHED THEN UPDATE
SET
      system_id = 				PRELOAD.system_id,
      brand_id = 				PRELOAD.brand_id,
      reservation_cd = 			PRELOAD.reservation_cd,
      reservation_create_dt = 	PRELOAD.reservation_create_dt,
      res_last_modify_dt = 		PRELOAD.res_last_modify_dt,
      res_notes = 				PRELOAD.res_notes,
      res_proj_co_dt = 			PRELOAD.res_proj_co_dt, 
      res_proj_ci_dt = 			PRELOAD.res_proj_ci_dt,
      last_name =  				PRELOAD.last_name,
      first_name =  			PRELOAD.first_name,
      flight_nbr = 				PRELOAD.flight_nbr, 
      load_ts =  				PRELOAD.load_ts, 
      euro_resnum = 			PRELOAD.euro_resnum,
      spcl_instrs =  			PRELOAD.spcl_instrs,
      rent_doc_cd =   			PRELOAD.rent_doc_cd,
      cash_in_club_nbr =   		PRELOAD.cash_in_club_nbr,
      ext_product_cd =   		PRELOAD.ext_product_cd, 
      caller_email =  			PRELOAD.caller_email, 
      agent_email =   			PRELOAD.agent_email, 
      caller_name =  			PRELOAD.caller_name,
      ptp_eligible_flg =   		PRELOAD.ptp_eligible_flg,
      record_locator =   		PRELOAD.record_locator, 
      external_reference_cd = 	PRELOAD.external_reference_cd, 
      voucher_cd =  			PRELOAD.voucher_cd, 
      sem_keyword =  			PRELOAD.sem_keyword,
      sem_click_tmsp =   		PRELOAD.sem_click_tmsp,
      sem_conversion_tmsp = 	PRELOAD.sem_conversion_tmsp, 
      membership_cd =  			PRELOAD.membership_cd,
      booking_channel_desc = 	PRELOAD.booking_channel_desc, 
      iata_arc_cd =  			PRELOAD.iata_arc_cd, 
      resv_rentr_recog_lvl_cd = PRELOAD.resv_rentr_recog_lvl_cd,
      resv_rentr_recog_id = 	PRELOAD.resv_rentr_recog_id,
      resv_create_cntrl_tmsp =  PRELOAD.resv_create_cntrl_tmsp,
      resv_last_mod_cntrl_tmsp = PRELOAD.resv_last_mod_cntrl_tmsp, 
      co_station_cd =  			PRELOAD.co_station_cd,
      ci_station_cd =   		PRELOAD.ci_station_cd, 
      res_station_taken_cd =   	PRELOAD.res_station_taken_cd, 
      erac_co_branch_lgcy_cd =  PRELOAD.erac_co_branch_lgcy_cd, 
      erac_ci_branch_lgcy_cd =  PRELOAD.erac_ci_branch_lgcy_cd,
      erac_resv_taken_branch_lgcy_cd = PRELOAD.erac_resv_taken_branch_lgcy_cd, 
      row_err_ind = 			PRELOAD.row_err_ind,
      rated_account_cd =   		PRELOAD.rated_account_cd,
      rated_account_id =		PRELOAD.rated_account_id, 
      rated_analysis_type_cd =  PRELOAD.rated_analysis_type_cd,
      reporting_analysis_type_cd = PRELOAD.reporting_analysis_type_cd, 
      international_trans_flg = PRELOAD.international_trans_flg,
      loyalty_club_flg =  		PRELOAD.loyalty_club_flg,
      org_role_cd =  			PRELOAD.org_role_cd,
      pcard_first_6_txt =  		PRELOAD.pcard_first_6_txt,
      pcard_last_4_txt = 		PRELOAD.pcard_last_4_txt,
      trans_id =  				PRELOAD.trans_id,
      booked_by_usr_cd =  		PRELOAD.booked_by_usr_cd,
      booked_by_usr_branch_lgcy_cd = PRELOAD.booked_by_usr_branch_lgcy_cd


WHEN NOT MATCHED THEN INSERT
(  	
      res_id, 
      system_id, 
      brand_id, 
      reservation_cd, 
      reservation_create_dt, 
      res_last_modify_dt, 
      res_notes, 
      res_proj_co_dt,
      res_proj_ci_dt,
      last_name,
      first_name,
      flight_nbr, 
      load_ts, 
      euro_resnum, 
      spcl_instrs, 
      rent_doc_cd, 
      cash_in_club_nbr, 
      ext_product_cd, 
      caller_email, 
      agent_email, 
      caller_name, 
      ptp_eligible_flg, 
      record_locator, 
      external_reference_cd, 
      voucher_cd, 
      sem_keyword, 
      sem_click_tmsp, 
      sem_conversion_tmsp, 
      membership_cd, 
      booking_channel_desc, 
      iata_arc_cd, 
      resv_rentr_recog_lvl_cd, 
      resv_rentr_recog_id, 
      resv_create_cntrl_tmsp, 
      resv_last_mod_cntrl_tmsp, 
      co_station_cd, 
      ci_station_cd, 
      res_station_taken_cd, 
      erac_co_branch_lgcy_cd, 
      erac_ci_branch_lgcy_cd, 
      erac_resv_taken_branch_lgcy_cd, 
      row_err_ind, 
      rated_account_cd, 
      rated_account_id,
      rated_analysis_type_cd, 
      reporting_analysis_type_cd, 
      international_trans_flg, 
      loyalty_club_flg,
      org_role_cd, 
      pcard_first_6_txt, 
      pcard_last_4_txt, 
      trans_id, 
      booked_by_usr_cd, 
      booked_by_usr_branch_lgcy_cd
)
VALUES
( 
      PRELOAD.res_id, 
      PRELOAD.system_id, 
      PRELOAD.brand_id, 
      PRELOAD.reservation_cd, 
      PRELOAD.reservation_create_dt, 
      PRELOAD.res_last_modify_dt, 
      PRELOAD.res_notes, 
      PRELOAD.res_proj_co_dt,
      PRELOAD.res_proj_ci_dt,
      PRELOAD.last_name,
      PRELOAD.first_name,
      PRELOAD.flight_nbr, 
      PRELOAD.load_ts, 
      PRELOAD.euro_resnum, 
      PRELOAD.spcl_instrs, 
      PRELOAD.rent_doc_cd, 
      PRELOAD.cash_in_club_nbr, 
      PRELOAD.ext_product_cd, 
      PRELOAD.caller_email, 
      PRELOAD.agent_email, 
      PRELOAD.caller_name, 
      PRELOAD.ptp_eligible_flg, 
      PRELOAD.record_locator, 
      PRELOAD.external_reference_cd, 
      PRELOAD.voucher_cd, 
      PRELOAD.sem_keyword, 
      PRELOAD.sem_click_tmsp, 
      PRELOAD.sem_conversion_tmsp, 
      PRELOAD.membership_cd, 
      PRELOAD.booking_channel_desc, 
      PRELOAD.iata_arc_cd, 
      PRELOAD.resv_rentr_recog_lvl_cd, 
      PRELOAD.resv_rentr_recog_id, 
      PRELOAD.resv_create_cntrl_tmsp, 
      PRELOAD.resv_last_mod_cntrl_tmsp, 
      PRELOAD.co_station_cd, 
      PRELOAD.ci_station_cd, 
      PRELOAD.res_station_taken_cd, 
      PRELOAD.erac_co_branch_lgcy_cd, 
      PRELOAD.erac_ci_branch_lgcy_cd, 
      PRELOAD.erac_resv_taken_branch_lgcy_cd, 
      PRELOAD.row_err_ind, 
      PRELOAD.rated_account_cd, 
      PRELOAD.rated_account_id,
      PRELOAD.rated_analysis_type_cd, 
      PRELOAD.reporting_analysis_type_cd, 
      PRELOAD.international_trans_flg, 
      PRELOAD.loyalty_club_flg,
      PRELOAD.org_role_cd, 
      PRELOAD.pcard_first_6_txt, 
      PRELOAD.pcard_last_4_txt, 
      PRELOAD.trans_id, 
      PRELOAD.booked_by_usr_cd, 
      PRELOAD.booked_by_usr_branch_lgcy_cd
);

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE


  /******************************************************/
  /* UPDATE DIM_RES_DOCUMENT - MASK DATA PROTECTION */
  /******************************************************/
  

update	DRD
FROM INTGRT_RPT_TB.DIM_RES_DOCUMENT DRD, 	
INTGRT_RPT_WT.DIM_RES_DOC_DPA_TD R   		
set last_name 	= R.last_name,
first_name 		= R.first_name,
caller_email 	= R.caller_email,
agent_email 	= R.agent_email,
caller_name 	= R.caller_name,
pcard_first_6_txt = R.pcard_first_6_txt,
pcard_last_4_txt  = R.pcard_last_4_txt,
ra_ins_nam 		= R.last_name
WHERE
DRD.res_id = R.res_id;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE



.LOGOFF;
.QUIT ERRORCODE


!@!

# capture the return code
rc=$?

case $rc in
    0 ) echo "c completed successful." >> $LOGFILE
     ;;
    * ) echo "BTEQ error while running Data Update for Dim Res Document " >> $LOGFILE
    ;;
esac

echo "**********************************************************************************************"
echo "* Log file may be found in $LOGFILE"
echo "**********************************************************************************************"
cat $LOGFILE

exit $rc