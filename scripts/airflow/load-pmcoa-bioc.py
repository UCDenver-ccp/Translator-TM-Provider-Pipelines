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
import csv
import re

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

dag = DAG(dag_id='load-pmcoa-bioc-dag', default_args=args, catchup=False, schedule_interval='0 12 * * 1',)

pmc_bioc_to_load_dir = "/home/airflow/gcs/data/pmcoa_load/bioc/to_load"

# # download the PMCOA oa_file_list.csv file
# download_oa_file_list_op = BashOperator(
#      task_id='download-oa-file-list',
#      bash_command="cd /home/airflow/gcs/data/pmcoa_load/bioc && wget https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_file_list.csv",
#      dag=dag)

# # extract PMC IDs from the oa_file_list.csv file and write to available.pmcids
# # to avoid some old documents that are only available by PDF, we restrict the date to be > 1975
# def extract_year(citation):
#     d = re.findall('(\d{4})', citation)
#     if len(d) > 0:
#         return int(d[0])
#     else:
#         print(f'Unable to extract year from citation: {citation}')
#         return 1800

# def extract_pmcids(**kwargs):
#     oa_file_list_file = "/home/airflow/gcs/data/pmcoa_load/bioc/oa_file_list.csv"
#     output_file = "/home/airflow/gcs/data/pmcoa_load/bioc/available.pmcids"
#     with open(oa_file_list_file , "rt") as csv_file, open(output_file, "wt") as pmcids_file:
#         csv_reader = csv.reader(csv_file, delimiter=',')
#         line_count = 0
#         for row in csv_reader:
#             if line_count > 0:
#                 year = extract_year(row[1])
#                 if year > 1975:
#                     pmcids_file.write(f'{row[2]}\n')
#             line_count += 1

# get_available_pmcids_op = PythonOperator(
#         task_id='extract_available_pmcids',
#         python_callable=extract_pmcids,
#         provide_context=True,
#         dag=dag)

# # take the set difference from the available.pmcids file and the existing.pmcids file to determine which files need to be downloaded
# compile_pmcids_to_download_op = BashOperator(
#     task_id='compile_pmcids_to_download',
#     bash_command="cd /home/airflow/gcs/data/pmcoa_load/bioc && comm -23 <(sort available.pmcids) <(sort existing.pmcids) > to_download.pmcids",
#     dag=dag)

# # download only new files using wget
# download_op = BashOperator(
#     task_id='download-pmcoa-bioc-xml-files',
#     bash_command="cd /home/airflow/gcs/data/pmcoa_load/bioc && mkdir -p /home/airflow/gcs/data/pmcoa_load/bioc/to_load && while read id; do wget -O \"to_load/$id.xml\" https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi/BioC_xml/$id/unicode; sleep 0.5; done <to_download.pmcids",
#     dag=dag)

# delete files with zero size
def delete_empty_files(**kwargs):
    for f in [os.path.join(pmc_bioc_to_load_dir, file) for file in os.listdir(pmc_bioc_to_load_dir)]:
        file_stats = os.stat(f)
        if file_stats.st_size == 0:
            # then we delete the file
            os.remove(f)

delete_empty_downloaded_files_op = PythonOperator(
        task_id='delete_empty_downloaded_files',
        python_callable=delete_empty_files,
        provide_context=True,
        dag=dag)


# # checks to see if the to_load directory is empty
# def check_for_files_to_process(**kwargs):
#     files = os.listdir(pmc_bioc_to_load_dir)
#     if len(files) > 0:
#         return 'dataflow_bioc_xml_to_text_op'
#     return 'pipeline_end'

# # checks to see if the to_load/ directory is empty. If not empty,
# # then returns the id for the dataflow_medline_xml_to_text task.
# check_for_files_to_process_op = BranchPythonOperator(
#         task_id='check_for_files_to_process',
#         python_callable=check_for_files_to_process,
#         provide_context=True,
#         dag=dag)

# ## call dataflow medline-xml-to-text
# dataflow_bioc_xml_to_text_op = DataflowCreateJavaJobOperator(
#     task_id="dataflow_bioc_xml_to_text",
#     jar=TM_PIPELINES_JAR,
#     job_name='{{task.task_id}}',
#     options={
#         'biocDir': PMCOA_BIOC_XML_DIR,
#         'collection': 'PMCOA',
#         'overwrite': 'NO',
#         'numWorkers': 10,
#         'workerMachineType': 'n1-highmem-2'
#     },
#     poll_sleep=10,
#     job_class='edu.cuanschutz.ccp.tm_provider.etl.BiocToTextPipeline',
#     check_if_running=CheckJobRunning.IgnoreJob,
#     location=DATAFLOW_REGION,
#     retries=0,
#     dag=dag
# )

# ## call dataflow filter-unactionable-text
# dataflow_filter_unactionable_text_op = DataflowCreateJavaJobOperator(
#     task_id="dataflow_filter_unactionable_text",
#     jar=TM_PIPELINES_JAR,
#     job_name='{{task.task_id}}',
#     options={
#         'textPipelineKey': BIOC_TO_TEXT,
#         'textPipelineVersion': 
#         'outputPipelineVersion':
#         'collection': 'PMCOA',
#         'optionalDocumentSpecificCollection':
#         'overwrite': 'NO',
#         'numWorkers': 10,
#         'workerMachineType': 'n1-highmem-2'
#     },
#     poll_sleep=10,
#     job_class='edu.cuanschutz.ccp.tm_provider.etl.BiocToTextPipeline',
#     check_if_running=CheckJobRunning.IgnoreJob,
#     location=DATAFLOW_REGION,
#     retries=0,
#     dag=dag
# )


# update the list of existing pmcids at the end once the load has completed
# update_existing_pmcids_op = BashOperator(
#     task_id='update-existing-pmcids',
#     bash_command="cd /home/airflow/gcs/data/pmcoa_load/bioc && find to_load/ -type f -name '*.xml' | grep PMC | cut -d '/' -f 2 | tr -d '.xml' >> existing.pmcids"
#     dag=dag)


# If all upstream tasks succeed, then remove files from the to_load directory.
# If any of the upstream processes failed then the files will be kept in the 
# to_load directory so that they are processed the next time the workflow runs.
pipeline_end_op = BashOperator(
    task_id='pipeline_end',
    bash_command=f"rm {pmc_bioc_to_load_dir}/*",
    dag=dag)


# get_available_pmcids_op.set_upstream(download_oa_file_list_op)
# compile_pmcids_to_download_op.set_upstream(get_available_pmcids_op)
# download_op.set_upstream(compile_pmcids_to_download_op)
# delete_empty_downloaded_files_op.set_upstream(download_op)
check_for_files_to_process_op.set_upstream(delete_empty_downloaded_files_op)
pipeline_end_op.set_upstream(check_for_files_to_process_op)

dataflow_bioc_xml_to_text_op.set_upstream(check_for_files_to_process_op)
dataflow_bioc_text_to_actionable_text_op.set_upstream(dataflow_bioc_xml_to_text_op)

#update_existing_pmcids_op.set_upstream(dataflow_bioc_text_to_actionable_text_op)
# pipeline_end_op.set_upstream(update_existing_pmcids_op)