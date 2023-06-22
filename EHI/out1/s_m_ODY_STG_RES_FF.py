# This is a standard header for workflow s_m_ODY_STG_RES_FF
from datetime import datetime, timedelta
import os
import pendulum
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable



Template file not found for component type COMMAND. component name: CMD_CREATE_RES2_FF

Template file not found for component type SESSION. component name: S_m_RES_REJ_FF_TO_ODYRPT

Template file not found for component type SESSION. component name: s_m_ODY_STG_RES_FF


########### Flow definition ###########


S_m_RES_REJ_FF_TO_ODYRPT << CMD_CREATE_RES2_FF

s_m_ODY_STG_RES_FF << S_m_RES_REJ_FF_TO_ODYRPT