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
# | XML-to-TEXT processing of PMCOA BioC files. This workflow                 |
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
# environment: /home/airflow/gcs/data/to_load_pmcoa_bioc_xml
PMCOA_BIOC_XML_DIR=os.environ.get('PMCOA_BIOC_XML_DIR')

# TM_PIPELINES_JAR = the path to the tm-pipelines jar file (in some bucket)
TM_PIPELINES_JAR=os.environ.get('TM_PIPELINES_JAR')

#==================DAG ARGUMENTS==============================

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 8, 8),
     # run this dag Mondays @ 6am MT which is 12pm UTC
    'schedule_interval': '0 12 * * 1',
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'dataflow_default_options': {
        'zone': DATAFLOW_ZONE,
        'region': DATAFLOW_REGION,
        'stagingLocation': DATAFLOW_STAGING_LOCATION,
        'gcpTempLocation': DATAFLOW_TMP_LOCATION,
    }
}

dag = DAG(dag_id='load-pmcoa-bioc-xml-dag', default_args=args, catchup=False, schedule_interval='0 12 * * 1',)

# download the PMCOA oa_file_list.csv file and parse out the available PMC IDs
get_available_pmcids_op = BashOperator(
    task_id='download-oa-file-list',
    bash_command="cd /home/airflow/gcs/data && wget https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_file_list.csv && cut -f 3 -d ',' oa_file_list.csv > available.pmcids"
    dag=dag)

# catalog existing PMC IDs, i.e. PMC IDs that have been previously downloaded
catalog_existing_pmcids_op = BashOperator(
    task_id='catalog-existing-pmcids',
    bash_command="cd /home/airflow/gcs/data && cp existing_bulk_apr2022.pmcids existing.pmcids && find updates/ -type f -name '*.xml' | grep PMC | cut -d '/' -f 2 | tr -d '.xml' >> existing.pmcids"
    dag=dag)

# take the set difference from the available.pmcids file and the existing.pmcids file to determine which files need to be downloaded
compile_pmcids_to_download_op = BashOperator(
    task_id='compile_pmcids_to_download',
    bash_command="cd /home/airflow/gcs/data && comm -23 <(sort available.pmcids) <(sort existing.pmcids) > to_download.pmcids"
    dag=dag)

# download only new files using wget
download_op = BashOperator(
    task_id='download-pmcoa-bioc-xml-files',
    bash_command="cd /home/airflow/gcs/data && while read id; do; wget https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi/BioC_xml/$id/unicode; done <to_download.pmcids",
    dag=dag)


### TO FIX: the to_download.pmcids file is just the ids, and below we need the file names to copy
### Question: where are the bioc files being downloaded? just into the data/ directory? I don't remember what that is...

populate_load_directory_op = BashOperator(
    task_id='populate-load-directory',
    # bash_command="cd /home/airflow/gcs/data && if [ -d /home/airflow/gcs/data/to_load ]; then rm -Rf /home/airflow/gcs/data/to_load; fi && mkdir /home/airflow/gcs/data/to_load && if [ -s downloaded-files.txt ]; then xargs -a downloaded-files.txt cp -t /home/airflow/gcs/data/to_load; fi",
    bash_command="cd /home/airflow/gcs/data && mkdir -p /home/airflow/gcs/data/to_load_pmcoa_bioc_xml && if [ -s to_download.pmcids ]; then xargs -a downloaded-files.txt cp -t /home/airflow/gcs/data/to_load_pmcoa_bioc_xml; fi",
    dag=dag)

# checks to see if the to_load directory is empty
def check_for_files_to_process(**kwargs):
    dir = os.listdir("/home/airflow/gcs/data/to_load_pmcoa_bioc_xml")
    if len(dir) > 0:
        return 'dataflow_bioc_xml_to_text'
    return 'pipeline_end'

# checks to see if the to_load/ directory is empty. If not empty,
# then returns the id for the dataflow_medline_xml_to_text task.
check_for_files_to_process = BranchPythonOperator(
        task_id='check_for_files_to_process',
        python_callable=check_for_files_to_process,
        provide_context=True,
        dag=dag)

## call dataflow medline-xml-to-text
dataflow_bioc_xml_to_text_op = DataflowCreateJavaJobOperator(
    task_id="dataflow_bioc_xml_to_text",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'biocDir': PMCOA_BIOC_XML_DIR,
        'collection': 'PMCOA',
        'overwrite': 'NO',
        'numWorkers': 10,
        'workerMachineType': 'n1-highmem-2'
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.BiocToTextPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)


# If all upstream tasks succeed, then remove files from the to_load directory.
# If any of the upstream processes failed then the files will be kept in the 
# to_load directory so that they are processed the next time the workflow runs.
pipeline_end_op = BashOperator(
    task_id='pipeline_end',
    bash_command="rm /home/airflow/gcs/data/to_load_pmcoa_bioc_xml/*",
    dag=dag)


# download --> verify_md5 --> populate_load_directory --> 
#      check_for_files_to_process --> dataflow_medline_xml_to_text --> pipeline_end
#      check_for_files_to_process --> pipeline_end

verify_md5_op.set_upstream(download_op)
populate_load_directory_op.set_upstream(verify_md5_op)
wrap_with_cdata_op.set_upstream(populate_load_directory_op)
check_for_files_to_process_op.set_upstream(wrap_with_cdata_op)
dataflow_medline_xml_to_text_op.set_upstream(check_for_files_to_process_op)
dataflow_medline_xml_to_text_op.set_downstream(pipeline_end_op)
check_for_files_to_process_op.set_downstream(pipeline_end_op)

