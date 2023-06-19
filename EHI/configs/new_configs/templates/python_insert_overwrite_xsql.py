# BEGIN Notebook initialization section
#
# FIXME databricks.migration.task Update 'File/Folder Location' for Databricks workspace.
dbutils.widgets.text("ofolder_%NUM%", "%FILE_NAME%")
# END Notebook initialization section

# OUTPUT to DIRECTORY 
# Note: Spark output target is always a folder not a file
#       Number of data files in output folder depends on 
#       data partitions (or explcit PARTITION hint to select clause)
#       Ref: https://docs.databricks.com/sql/language-manual/sql-ref-syntax-qry-select-hints.html

# FIXME databricks.migration.task Review/Update 'File Format/Options' for Databricks.
export_sqlstr_%NUM% = """
INSERT OVERWRITE DIRECTORY '{outputDir}'
USING PARQUET
SELECT /*+ {partitionHint} */ 
%SELECT_STATEMENT%
""".format(
  outputDir = dbutils.widgets.get("ofolder_%NUM%"),
  partitionHint = "REPARTITION(1)" 
)

xSqlStmt.execute(export_sqlstr_%NUM%, verbose=True)
