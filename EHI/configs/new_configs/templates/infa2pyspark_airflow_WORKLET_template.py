%UPSTREAM_COMPONENT_LIST% = TriggerDagRunOperator(
        task_id = "%UPSTREAM_COMPONENT_LIST%",
        trigger_dag_id = "%COMPONENT_NAME%",
        execution_date = "{{ ds }}",
        reset_dag_run = True
    )