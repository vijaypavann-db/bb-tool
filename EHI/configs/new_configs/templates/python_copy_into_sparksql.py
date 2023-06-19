# BEGIN Notebook initialization section
#
# Databricks notebook source
# DBTITLE 1, Notebook initialization section
# FIXME databricks.migration.task Update 'File/Folder Location' for Databricks workspace.
dbutils.widgets.text("ifile_%NUM%", "%FILE_NAME%")
# FIXME databricks.migration.task Update 'File/Folder Location' for Databricks workspace.
<?BAD_RECS_PATH:>dbutils.widgets.text("badrecs_path_%NUM%", "%BAD_RECS_PATH%")</BAD_RECS_PATH>
# END Notebook initialization section

#
# DBTITLE 1, COPY INTO %TABLE_NAME%
# FIXME databricks.migration.task Review/Update 'File Format/Options' for Databricks.
# FIXME databricks.migration.task Review/Update as needed.
spark.sql("""
  COPY INTO %TABLE_NAME%
  FROM (
    SELECT
        %COLUMN_NAMES%
    FROM '{ifile}'
  )
  FILEFORMAT = %FILE_FORMAT%
  FORMAT_OPTIONS (
     <?DELIMITER:>'delimiter'='%DELIMITER%', 'inferSchema'='true',</DELIMITER>
     <?BAD_RECS_PATH:>'badRecordsPath' = '{badrecs_path}'</BAD_RECS_PATH>
  )
""".format(
  ifile = dbutils.widgets.get('ifile_%NUM%')<?BAD_RECS_PATH:>,
  badrecs_path = dbutils.widgets.get('badrecs_path_%NUM%')</BAD_RECS_PATH>
  )
).show(truncate=False)

__END__ Everything from the "__END__" line onward is removed during processing, so we can put user info here

Note about this syntax: <?tag:> %tag% </tag>
   When %tag% does not convert to anything during processing, then everything from "<?tag:>" to "</tag>" will
   be removed. 
