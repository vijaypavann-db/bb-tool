# This is a standard header for workflow wf_DIM_RES_DOCUMENT_ECARS2
from datetime import datetime, timedelta
import os
import pendulum
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable



Template file not found for component type SESSION.


########### Flow definition ###########
