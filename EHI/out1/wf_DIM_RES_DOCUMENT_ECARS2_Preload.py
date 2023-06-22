# This is a standard header for workflow wf_DIM_RES_DOCUMENT_ECARS2_Preload
from datetime import datetime, timedelta
import os
import pendulum
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable



Template file not found for component type SESSION. component name: s_m_DIM_RES_DOCUMENT_ECARS2_WORK1_Preload

Template file not found for component type SESSION. component name: s_m_DIM_RES_DOCUMENT_ECARS2_WORK2_Preload

Template file not found for component type SESSION. component name: s_m_DIM_RES_DOCUMENT_ECARS2_WORK3_Preload

Template file not found for component type SESSION. component name: s_m_DIM_RES_DOCUMENT_ECARS2_WORK4_Preload

Template file not found for component type SESSION. component name: s_m_DIM_RES_DOCUMENT_ECARS2_WORK5_Preload

Template file not found for component type SESSION. component name: s_m_DIM_RES_DOCUMENT_ECARS2_WORK6_Preload

Template file not found for component type SESSION. component name: s_m_DIM_RES_DOCUMENT_ECARS2_WORK7_Preload


########### Flow definition ###########


s_m_DIM_RES_DOCUMENT_ECARS2_WORK6_Preload << s_m_DIM_RES_DOCUMENT_ECARS2_WORK5_Preload

s_m_DIM_RES_DOCUMENT_ECARS2_WORK4_Preload << s_m_DIM_RES_DOCUMENT_ECARS2_WORK3_Preload

s_m_DIM_RES_DOCUMENT_ECARS2_WORK2_Preload << s_m_DIM_RES_DOCUMENT_ECARS2_WORK1_Preload

s_m_DIM_RES_DOCUMENT_ECARS2_WORK7_Preload << s_m_DIM_RES_DOCUMENT_ECARS2_WORK6_Preload

s_m_DIM_RES_DOCUMENT_ECARS2_WORK5_Preload << s_m_DIM_RES_DOCUMENT_ECARS2_WORK4_Preload

s_m_DIM_RES_DOCUMENT_ECARS2_WORK3_Preload << s_m_DIM_RES_DOCUMENT_ECARS2_WORK2_Preload