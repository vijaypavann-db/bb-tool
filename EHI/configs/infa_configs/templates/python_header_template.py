#----------------------------------------------------------------------------------------
#--Script Function : %JOB_DESCRIPTION%
#--Script Created By : <HUB_ID>
#--Script Creation Date :
#--Script Version : 1.0
#--Input :
#--Output :
#--Arguments :
#--Parameters: <work db> [${WORK_DB}] database where temporary work tables are created
#--            <target db> [${TARGET_DB}] ultimate database being populated
#--            <target vmdb> [${TARGET_VMDB}] view database to above
#--            <ETL_PROC_NM> [${task_nm}]
#--            <ETL_PROC_RUN_ID>  [${run_id}]
#--            <ETL_TARG_REC_SET_ID> [${ETL_TARG_REC_SET_ID}]
#--            <SRC_SYS_NM> [${SRC_SYS_NM}]
#----------------------------------------------------------------------------------------
import os
import sys
sys.path.append("../..")
import snowflake.connector 
#Error handling to be encapsulated within SFWrapper 
from scripts.wrapper.sfwrapper import SFWrapper 
import snowflake.connector #Error handling to be encapsulated within SFWrapper 
from Mapplets import Mapplets
#from sfwrapper_updated import SFWrapper
# import for handling gotos
from goto import with_goto
import logging

class SFRun(object):

    def __init__(self,task_id,batch_id,run_id):
        super().__init__()
        self.dbh = SFWrapper(task_id,batch_id,run_id)


    # added goto decorator and wrapped everything in a function
    @with_goto
    def entry(self):

        #initialize statement counter, so we can keep track of statements.
        statement_id = ''

        self.dbh.connect()

        try:

            self.dbh.exec(""" USE DATABASE ${trgt_db_nm};""")
        
            self.dbh.exec(""" ALTER SESSION SET TIMESTAMP_NTZ_OUTPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6';""")
