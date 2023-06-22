# This is a standard header for workflow s_m_DWH_STG_RES_CHANNEL
from datetime import datetime, timedelta
import os
import pendulum
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable



Template file not found for component type SESSION. component name: s_m_DIM_CUSTOMER_DRIVER_RES_ADD

Template file not found for component type SESSION. component name: s_m_DWH_STG_RES_CHANNEL2


########### Flow definition ###########


s_m_DWH_STG_RES_CHANNEL2 << s_m_DIM_CUSTOMER_DRIVER_RES_ADD