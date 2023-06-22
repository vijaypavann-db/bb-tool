# BEGIN Notebook initialization section
#
# FIXME databricks.migration.task Update 'File/Folder Location' for Databricks workspace.
dbutils.widgets.text("ifile_%NUM%", "%FILE_NAME%")
# END Notebook initialization section

#
# COPY INTO COMAMND
#
# FIXME databricks.migration.task Review/Update 'File Format/Options' for Databricks.
copy_into_sqlstr_%NUM% = """
  COPY INTO %TABLE_NAME%
  FROM (
    SELECT
        %COLUMN_NAMES%
    FROM '{}'
  )
  FILEFORMAT = %FILE_FORMAT%
  FORMAT_OPTIONS (
     <?DELIMITER:>'delimiter'='%DELIMITER%', 'inferSchema'='true'</DELIMITER>
     <?BAD_RECS_PATH:>'badRecordsPath' = '{badrecs_path}'</BAD_RECS_PATH>
  )
""".format(
  dbutils.widgets.get('ifile_%NUM%')
)
xSqlStmt.execute(copy_into_sqlstr_%NUM%, verbose=True)

__END__ Everything from the "__END__" line onward is removed during processing, so we can put user info here

Note about this syntax: <?tag:> %tag% </tag>
   When %tag% does not convert to anything during processing, then everything from "<?tag:>" to "</tag>" will
   be removed. 

