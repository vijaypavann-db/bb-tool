%COMPONENT_NAME%= SparkSubmitOperator(task_id='%COMPONENT_NAME%',
conn_id='spark_local',
application=f'{pyspark_app_home}/spark/%MAPPING_NAME%.py',
total_executor_cores=4,
executor_cores=2,
executor_memory='5g',
driver_memory='5g',
name='%COMPONENT_NAME%',
execution_timeout=timedelta(minutes=60),
dag=dag
)
