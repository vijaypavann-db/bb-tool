# This is a standard header for workflow wf_DIM_RENT_DOCUMENT_TD
from datetime import datetime, timedelta
import os
import pendulum
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable



Template file not found for component type SESSION. component name: s_m_DIM_RENT_DOCUMENT_TD

Template file not found for component type SESSION. component name: s_m_ODY_DIM_RENT_DOCUMENT_DPA_MASK_TD

Template file not found for component type COMMAND. component name: triggerscp_dim_rent_document_handshake


########### Flow definition ###########


triggerscp_dim_rent_document_handshake << s_m_ODY_DIM_RENT_DOCUMENT_DPA_MASK_TD

s_m_DIM_RENT_DOCUMENT_TD << e_Wait_DIM_RENT_DOCUMENT_Trigger

s_m_ODY_DIM_RENT_DOCUMENT_DPA_MASK_TD << s_m_DIM_RENT_DOCUMENT_TD