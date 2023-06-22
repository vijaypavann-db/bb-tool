%python
# DBTITLE 1, Notebook Initializations

import decimal, json

<?ARG_DEF:>
dbutils.widgets.text("%ARG_NAME%", "%ARG_VALUE%")
</ARG_DEF>
<?ARG_GET:>
%ARG_NAME% = %ARG_DATA_TYPE%( dbutils.widgets.get("%ARG_NAME%") )
</ARG_GET>

<?VAR_DECL:>
%VAR_NAME% = %VAR_DEFAULT_VALUE%
</VAR_DECL>
-- COMMAND ----------
<?ERROR_HANDLER:>
# COMMAND ----------
# DBTITLE 1, error_handler
def error_handler%ERROR_HANDLER_NUM%(e):
  # FIXME databricks.migration.task  Review and Update exception handling for Databricks 
  #  %ERROR_CONDITION%
%ERROR_ACTION%
</ERROR_HANDLER>

<?CONTINUE_HANDLER:>
# COMMAND ----------
# FIXME databricks.migration.unsupported.feature Teradata 'CONTINUE HANDLER'
#
%DECLARE_CONTINUE%
</CONTINUE_HANDLER>

%TRY%

%BODY%

<?EXCEPT_BLOCK:>
# FIXME databricks.migration.task  Review and Update exception handling for Databricks 
# Call error hanlder and raise exception to stop notebook execution
except Exception as e:
  <?ERROR_HANDLER_CALL:>
  error_handler%ERROR_HANDLER_NUM%(e)
  raise
  </ERROR_HANDLER_CALL>
</EXCEPT_BLOCK>

<?RESULT_BLOCK:>
# COMMAND ----------
# return results
_result = {
  <?SET_RESULT_JSON:>
  "%VAR_NAME%": str(%VAR_NAME%),
  </SET_RESULT_JSON>
}
dbutils.notebook.exit(json.dumps(_result))
</RESULT_BLOCK>

__END__
