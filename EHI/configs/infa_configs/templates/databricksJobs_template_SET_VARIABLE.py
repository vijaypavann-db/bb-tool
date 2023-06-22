var_names = [%VARIABLE_NAME_LIST%]
var_values = [%VARIABLE_VALUE_LIST%]
varCount = 0
print("Executing component %JOB_NAME%.%COMPONENT_NAME%")
for varName in var_names:
    try:
        os.environ[varName] = var_values[varCount]
    except Exception as e:
        print("Execution error: " + str(e))
    varCount += 1
