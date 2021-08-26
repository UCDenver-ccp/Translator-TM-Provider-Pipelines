from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator  import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import DAG
from airflow.utils import dates
from datetime import datetime, timedelta

from airflow.providers.google.cloud.operators.dataflow import (
    CheckJobRunning,
    DataflowCreateJavaJobOperator,
)
from airflow.providers.google.cloud.sensors.dataflow import (
    DataflowJobAutoScalingEventsSensor,
    DataflowJobMessagesSensor,
    DataflowJobMetricsSensor,
    DataflowJobStatusSensor,
)

import os
import json
import pickle
import math
import pymongo
import csv
import gzip


# =============================================================================
# | This Airflow workflow facilitates the downloading and subsequent          |
# | XML-to-TEXT processing of MEDLINE daily update files. This workflow       |
# | downloads XML files that have not already been downloaded and kicks off   |
# | the XML-to-TEXT Dataflow pipeline to load those as documents into Cloud   |
# | Datastore.                                                                |
# =============================================================================


#====ENVIRONMENT VARIABLES THAT MUST BE SET IN CLOUD COMPOSER====
DATAFLOW_TMP_LOCATION=os.environ.get('DATAFLOW_TMP_LOCATION')
DATAFLOW_STAGING_LOCATION=os.environ.get('DATAFLOW_STAGING_LOCATION')
DATAFLOW_ZONE=os.environ.get('DATAFLOW_ZONE')
DATAFLOW_REGION=os.environ.get('DATAFLOW_REGION')

# MEDLINE_XML_DIR = the path to the bucket associated with the composer 
# environment: /home/airflow/gcs/data/to_load
MEDLINE_XML_DIR=os.environ.get('MEDLINE_XML_DIR')

# TM_PIPELINES_JAR = the path to the tm-pipelines jar file (in some bucket)
TM_PIPELINES_JAR=os.environ.get('TM_PIPELINES_JAR')

#==================DAG ARGUMENTS==============================

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 11),
     # run this dag daily @ midnight
    'schedule_interval': '0 0 * * *',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'dataflow_default_options': {
        'zone': DATAFLOW_ZONE,
        'region': DATAFLOW_REGION,
        'stagingLocation': DATAFLOW_STAGING_LOCATION,
        'gcpTempLocation': DATAFLOW_TMP_LOCATION,
    }
}

dag = DAG(dag_id='load-medline-xml-dag', default_args=args, catchup=False)

# download only new files using wget and log names of the downloaded files
# to downloaded-files.txt
download = BashOperator(
    task_id='download-pubmed-update-files',
    bash_command="cd /home/airflow/gcs/data && wget -N 'ftp://ftp.ncbi.nlm.nih.gov:21/pubmed/updatefiles/pubmed21n*' 2>&1  | grep done | grep '.gz' | grep -v '.gz.md5' | tr -s ' ' | cut -f 7 -d ' '  > downloaded-files.txt",
    dag=dag)

# TODO: the md5sum verify could be targeted to only the newly downloaded files
verify_md5 = BashOperator(
    task_id='verify-md5-checksums',
    bash_command="cd /home/airflow/gcs/data && md5sum --check *.gz.md5",
    dag=dag)

populate_load_directory = BashOperator(
    task_id='populate-load-directory',
    # bash_command="cd /home/airflow/gcs/data && if [ -d /home/airflow/gcs/data/to_load ]; then rm -Rf /home/airflow/gcs/data/to_load; fi && mkdir /home/airflow/gcs/data/to_load && if [ -s downloaded-files.txt ]; then xargs -a downloaded-files.txt cp -t /home/airflow/gcs/data/to_load; fi",
    bash_command="cd /home/airflow/gcs/data && mkdir -p /home/airflow/gcs/data/to_load && if [ -s downloaded-files.txt ]; then xargs -a downloaded-files.txt cp -t /home/airflow/gcs/data/to_load; fi",
    dag=dag)

# checks to see if the to_load directory is empty
def check_for_files_to_process(**kwargs):
    # if os.stat("/home/airflow/gcs/data/downloaded-files.txt").st_size > 0:
    dir = os.listdir("/home/airflow/gcs/data/to_load")
    if len(dir) > 0:
        return 'dataflow_medline_xml_to_text'
    return 'pipeline_end'

# checks to see if the to_load/ directory is empty. If not empty,
# then returns the id for the dataflow_medline_xml_to_text task.
check_for_files_to_process = BranchPythonOperator(
        task_id='check_for_files_to_process',
        python_callable=check_for_files_to_process,
        provide_context=True,
        dag=dag)

## call dataflow medline-xml-to-text
dataflow_medline_xml_to_text = DataflowCreateJavaJobOperator(
    task_id="dataflow_medline_xml_to_text",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'medlineXmlDir': MEDLINE_XML_DIR,
        'collection': 'PUBMED',
        'numWorkers': 10,
        'workerMachineType': 'n1-highmem-2'
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.MedlineXmlToTextPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)


# If all upstream tasks succeed, then remove files from the to_load directory.
# If any of the upstream processes failed then the files will be kept in the 
# to_load directory so that they are processed the next time the workflow runs.
pipeline_end = BashOperator(
    task_id='pipeline_end',
    bash_command="rm /home/airflow/gcs/data/to_load/*",
    dag=dag)


# download --> verify_md5 --> populate_load_directory --> 
#      check_for_files_to_process --> dataflow_medline_xml_to_text --> pipeline_end
#      check_for_files_to_process --> pipeline_end

verify_md5.set_upstream(download)
populate_load_directory.set_upstream(verify_md5)
check_for_files_to_process.set_upstream(populate_load_directory)
dataflow_medline_xml_to_text.set_upstream(check_for_files_to_process)
dataflow_medline_xml_to_text.set_downstream(pipeline_end)
check_for_files_to_process.set_downstream(pipeline_end)

