
# DBTITLE 1,Notebook initialization section
# FIXME databricks.migration.task Update 'File/Folder Location' for Databricks workspace.
dbutils.widgets.text("ifile_%NUM%", "%FILE_NAME%")

<?VIEW_FOR_MERGE:>
# COMMAND ----------
# DBTITLE 1, %FILE_NAME% inputfile
# FIXME databricks.migration.task Review/Update 'File Format/Options' for Databricks.
spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW INPUT_%NUM%
USING %FORMAT%
OPTIONS (
  path "${ifile}",
  <?DELIMITER:>
  sep %DELIMITER%,
  header "false",
  inferSchema "true",
  </DELIMITER>
  mode "FAILFAST" -- Abort with RuntimeException for any malformed records  
)
;
""".format(
   ifile = dbutils.widgets.get("ifile_%NUM%")
)).show(truncate=False)
</VIEW_FOR_MERGE>

<?MERGE_UPDATE:>
# COMMAND ----------
# DBTITLE 1, %TITLE%            -- From DML Label <labelname>
# FIXME databricks.migration.task Review/Update as needed.
spark.sql("""
MERGE INTO %TABLE_NAME% t
USING (
  SELECT
    %COLUMN_NAMES%
  FROM
    INPUT_%NUM%
  %WHERE%
) s
ON %ON%
WHEN MATCHED THEN
  UPDATE SET
    %UPDATE_SET%
;
""").show(truncate=False)
</MERGE_UPDATE>

<?MERGE_DELETE:>
# COMMAND ----------
# DBTITLE 1, %TITLE% 
# FIXME databricks.migration.task Review/Update as needed.
spark.sql("""
MERGE INTO %TABLE_NAME% t
USING (
  SELECT
    %COLUMN_NAMES%
  FROM
    INPUT_%NUM%
  %WHERE%
) s
ON %ON%
WHEN MATCHED THEN
  DELETE
;
""").show(truncate=False)
</MERGE_DELETE>

<?MERGE_UPSERT:>
# COMMAND ----------
# DBTITLE 1, %TITLE%
# FIXME databricks.migration.task Review/Update as needed.
spark.sql("""
MERGE INTO %TABLE_NAME% t
USING (
  SELECT
    %COLUMN_NAMES%
  FROM
    INPUT_%NUM% 
) s
ON %ON%
WHEN MATCHED THEN
  UPDATE SET
    %UPDATE_SET%
WHEN NOT MATCHED THEN 
  INSERT (
    %INSERT_COLUMN_NAMES%
   )
  VALUES (
    %INSERT_VALUES%
  )
;
""").show(truncate=False)
</MERGE_UPSERT>

<?COPY_INTO:>
# COMMAND ----------
# DBTITLE 1, %TITLE% 
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
FORMAT_OPTIONS ('mergeSchema'='true')
""".format(
  ifile=dbutils.widgets.get('ifile_%NUM%')
)).show(truncate=False)
</COPY_INTO>

__END__