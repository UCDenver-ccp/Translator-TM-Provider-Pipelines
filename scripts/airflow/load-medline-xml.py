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

from medline_xml_util import wrap_title_and_abstract_with_cdata


# =============================================================================
# | This Airflow workflow facilitates the downloading of MEDLINE daily update 
# | files. This workflow downloads XML files that have not already been 
# | downloaded and kicks off the XML-to-TEXT Dataflow pipeline to load those 
# | as documents into Cloud Datastore.
# =============================================================================

#====ENVIRONMENT VARIABLES THAT MUST BE SET IN CLOUD COMPOSER====
DATAFLOW_TMP_LOCATION=os.environ.get('DATAFLOW_TMP_LOCATION')
DATAFLOW_STAGING_LOCATION=os.environ.get('DATAFLOW_STAGING_LOCATION')
DATAFLOW_ZONE=os.environ.get('DATAFLOW_ZONE')
DATAFLOW_REGION=os.environ.get('DATAFLOW_REGION')

# MEDLINE_XML_DIR = the path to the bucket associated with the composer 
# environment: /home/airflow/gcs/data/to_load
MEDLINE_XML_DIR=os.environ.get('MEDLINE_XML_DIR')

TM_PIPELINES_JAR=os.environ.get('TM_PIPELINES_JAR')
TM_PIPELINES_JAR_NAME=os.environ.get('TM_PIPELINES_JAR_NAME')

COMPOSER_BUCKET_DATA_PATH=os.environ.get('COMPOSER_BUCKET_DATA_PATH')

#==================DAG ARGUMENTS==============================

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 11),
     # run this dag Mondays @ 3am MT which is 9am UTC
    'schedule_interval': '0 9 * * 1',
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'dataflow_default_options': {
        'zone': DATAFLOW_ZONE,
        'region': DATAFLOW_REGION,
        'stagingLocation': DATAFLOW_STAGING_LOCATION,
        'gcpTempLocation': DATAFLOW_TMP_LOCATION,
    }
}

dag = DAG(dag_id='load-medline-xml-dag', default_args=args, catchup=False, schedule_interval='0 9 * * 1',)

# download only new files using wget and log names of the downloaded files
# to downloaded-files.txt
download_op = BashOperator(
    task_id='download-pubmed-update-files',
    bash_command="mkdir -p /home/airflow/gcs/data/medline_load/update_files && cd /home/airflow/gcs/data/medline_load/update_files && wget -N 'ftp://ftp.ncbi.nlm.nih.gov:21/pubmed/updatefiles/pubmed23n*' 2>&1  | grep done | grep '.gz' | grep -v '.gz.md5' | tr -s ' ' | cut -f 7 -d ' '  > downloaded-files.txt",
    dag=dag)

# TODO: the md5sum verify could be targeted to only the newly downloaded files
verify_md5_op = BashOperator(
    task_id='verify-md5-checksums',
    bash_command="cd /home/airflow/gcs/data/medline_load/update_files && md5sum --check *.gz.md5",
    dag=dag)

populate_load_directory_op = BashOperator(
    task_id='populate-load-directory',
    bash_command="cd /home/airflow/gcs/data/medline_load/update_files && mkdir -p /home/airflow/gcs/data/medline_load/to_load && if [ -s downloaded-files.txt ]; then xargs -a downloaded-files.txt cp -t /home/airflow/gcs/data/medline_load/to_load; fi",
    dag=dag)

# wraps the article title and abstract text with CDATA fields - this is 
# necessary so that the XML parser doesn't fail when it encounters formatting
# elements, such as <b>, <i>, <sup>, etc., in the titles and abstracts.
#
# The python functions replace a previous approach based on sed. The original 
# sed commands are logged below for historical purposes only. The sed commands
# were introducing a CTRL-A in the replaced text for some reason - unsure why.
# #### sed -i 's/<ArticleTitle\([^>]*\)>/<ArticleTitle\1><![CDATA[/g' *.xml && 
# #### sed -i 's/<\/ArticleTitle>/]]><\/ArticleTitle>/g' *.xml && 
# #### sed -i 's/<AbstractText\([^>]*\)>/<AbstractText\1><![CDATA[/g' *.xml && 
# #### sed -i 's/<\/AbstractText>/]]><\/AbstractText>/g' *.xml",
wrap_with_cdata_op = PythonOperator(
        task_id='wrap_with_cdata',
        python_callable=wrap_title_and_abstract_with_cdata,
        provide_context=True,
        op_kwargs={'dir': '/home/airflow/gcs/data/medline_load/to_load'},
        dag=dag)

# checks to see if the to_load directory is empty
def check_for_files_to_process(**kwargs):
    dir = os.listdir("/home/airflow/gcs/data/medline_load/to_load")
    if len(dir) > 0:
        return 'wrap_with_cdata'
    return 'pipeline_end'

# checks to see if the to_load/ directory is empty. If not empty,
# then returns the id for the create_publication_metadata task.
check_for_files_to_process_op = BranchPythonOperator(
        task_id='check_for_files_to_process',
        python_callable=check_for_files_to_process,
        provide_context=True,
        dag=dag)

# call dataflow medline-xml-to-text to load the Medline record title/abstract into Cloud Datastore
dataflow_medline_xml_to_text_op = DataflowCreateJavaJobOperator(
    task_id="dataflow_medline_xml_to_text",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'medlineXmlDir': MEDLINE_XML_DIR,
        'collection': 'PUBMED',
        'pmidSkipFilePath': 'null',
        'overwrite': 'NO',
        'numWorkers': 10,
        'workerMachineType': 'n1-highmem-16'
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
pipeline_end_op = BashOperator(
    task_id='pipeline_end',
    bash_command="rm /home/airflow/gcs/data/medline_load/to_load/*",
    dag=dag)


verify_md5_op.set_upstream(download_op)
populate_load_directory_op.set_upstream(verify_md5_op)
check_for_files_to_process_op.set_upstream(populate_load_directory_op)
wrap_with_cdata_op.set_upstream(check_for_files_to_process_op)
dataflow_medline_xml_to_text_op.set_upstream(wrap_with_cdata_op)
pipeline_end_op.set_upstream(dataflow_medline_xml_to_text_op)
pipeline_end_op.set_upstream(check_for_files_to_process_op)




