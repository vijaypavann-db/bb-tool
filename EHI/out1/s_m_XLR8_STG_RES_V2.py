# This is a standard header for workflow s_m_XLR8_STG_RES_V2
from datetime import datetime, timedelta
import os
import pendulum
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable



Template file not found for component type COMMAND. component name: CMD_CREATE_RES2SELECT

Template file not found for component type SESSION. component name: s_m_RES_REJECTS_TO_ODYRPT

Template file not found for component type COMMAND. component name: COMMAND_CREATE_STG_RES_ODY

Template file not found for component type SESSION. component name: s_m_XLR8_STG_RES_V2

Template file not found for component type SESSION. component name: s_m_DWH_DIM_SOURCE_FACTRES


########### Flow definition ###########


COMMAND_CREATE_STG_RES_ODY << s_m_RES_REJECTS_TO_ODYRPT

s_m_DWH_DIM_SOURCE_FACTRES << s_m_XLR8_STG_RES_V2

s_m_RES_REJECTS_TO_ODYRPT << CMD_CREATE_RES2SELECT

s_m_XLR8_STG_RES_V2 << COMMAND_CREATE_STG_RES_ODY