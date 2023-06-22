# This is a standard header for workflow s_m_XLR8_DIM_RES_DOCUMENT
from datetime import datetime, timedelta
import os
import pendulum
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable



Template file not found for component type SESSION. component name: s_m_XLR8_DIM_RES_DOCUMENT

Template file not found for component type SESSION. component name: s_m_ODY_DIM_RES_DOCUMENT_DPA_MASK_EXT

Template file not found for component type SESSION. component name: s_m_ODY_DIM_RES_DOCUMENT_DPA_MASK_ORA


########### Flow definition ###########


s_m_ODY_DIM_RES_DOCUMENT_DPA_MASK_ORA << s_m_ODY_DIM_RES_DOCUMENT_DPA_MASK_EXT

s_m_ODY_DIM_RES_DOCUMENT_DPA_MASK_EXT << s_m_XLR8_DIM_RES_DOCUMENT