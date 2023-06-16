import os
command_name_list = [%COMMAND_NAME_LIST%]
command_list = [%COMMAND_LIST%]
commandCount = 0
for cmd in command_list:
    print("Executing command " + str(command_name_list[commandCount]))
    try:
        os.system(cmd)
    except Exception as e:
        print("Execution error: " + str(e))
    commandCount += 1
