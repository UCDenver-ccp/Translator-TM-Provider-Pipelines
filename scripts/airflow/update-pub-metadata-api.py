from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator  import BashOperator
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.hooks.base_hook import BaseHook
from airflow.models import DAG
# from airflow.utils import dates
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

from medline_xml_util import wrap_title_and_abstract_with_cdata
import os
# import gzip
# import re

# =============================================================================
# | This Airflow workflow updates the Publication Metadata API by processing  |
# | the MEDLINE daily update files.                                           |
# =============================================================================

#====ENVIRONMENT VARIABLES THAT MUST BE SET IN CLOUD COMPOSER====
# Note that Dataflow is not currently used by this pipeline,
# but the environment variable are included in case a DataflowOperator
# is added in the future
DATAFLOW_TMP_LOCATION=os.environ.get('DATAFLOW_TMP_LOCATION')
DATAFLOW_STAGING_LOCATION=os.environ.get('DATAFLOW_STAGING_LOCATION')
DATAFLOW_ZONE=os.environ.get('DATAFLOW_ZONE')
DATAFLOW_REGION=os.environ.get('DATAFLOW_REGION')

TM_PIPELINES_JAR=os.environ.get('TM_PIPELINES_JAR')
TM_PIPELINES_JAR_NAME=os.environ.get('TM_PIPELINES_JAR_NAME')

PUBLICATION_METADATA_BUCKET=os.environ.get('PUBLICATION_METADATA_BUCKET')
PUBLICATION_METADATA_PATH=os.environ.get('PUBLICATION_METADATA_PATH')

PUBLICATION_METADATA_API_URL=os.environ.get('PUBLICATION_METADATA_API_URL')
PUBLICATION_METADATA_SERVICE_HMAC_KEY_ID=os.environ.get('PUBLICATION_METADATA_SERVICE_HMAC_KEY_ID')
PUBLICATION_METADATA_SERVICE_HMAC_SECRET=os.environ.get('PUBLICATION_METADATA_SERVICE_HMAC_SECRET')

#==================DAG ARGUMENTS==============================

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 11),
     # run this dag daily @ 10pm MT which is 4am UTC
    'schedule_interval': '0 4 * * *',
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'dataflow_default_options': {
        'zone': DATAFLOW_ZONE,
        'region': DATAFLOW_REGION,
        'stagingLocation': DATAFLOW_STAGING_LOCATION,
        'gcpTempLocation': DATAFLOW_TMP_LOCATION,
    }
}

dag = DAG(dag_id='update-pub-metadata-api-dag', default_args=args, catchup=False, schedule_interval='0 4 * * *',)

# download only new files using wget and log names of the downloaded files
# to pub-metadata-update-downloaded-files.txt
download = BashOperator(
    task_id='download-pubmed-update-files',
    bash_command="cd /home/airflow/gcs/data/pub_metadata_2023/update_files && wget -N 'ftp://ftp.ncbi.nlm.nih.gov:21/pubmed/updatefiles/pubmed23n*' 2>&1  | grep done | grep '.gz' | grep -v '.gz.md5' | tr -s ' ' | cut -f 7 -d ' '  > pub-metadata-update-downloaded-files.txt",
    dag=dag)

# TODO: the md5sum verify could be targeted to only the newly downloaded files
verify_md5 = BashOperator(
    task_id='verify-md5-checksums',
    bash_command="cd /home/airflow/gcs/data/pub_metadata_2023/update_files && md5sum --check *.gz.md5",
    dag=dag)

# copy any newly downloaded Medline XML files to the to_load directory where they will be processed further
populate_load_directory = BashOperator(
    task_id='populate-load-directory',
    bash_command="cd /home/airflow/gcs/data/pub_metadata_2023/update_files && mkdir -p /home/airflow/gcs/data/pub_metadata_2023/to_load && if [ -s pub-metadata-update-downloaded-files.txt ]; then xargs -a pub-metadata-update-downloaded-files.txt cp -t /home/airflow/gcs/data/pub_metadata_2023/to_load; fi",
    dag=dag)

# checks to see if the to_load directory is empty
def check_for_files_to_process(**kwargs):
    load_dir = os.listdir("/home/airflow/gcs/data/pub_metadata_2023/to_load")
    if len(load_dir) > 0:
        return 'wrap_with_cdata'
    return 'pipeline_end'

# checks to see if the to_load/ directory is empty. If not empty,
# then returns the id for the create_publication_metadata task.
check_for_files_to_process = BranchPythonOperator(
        task_id='check_for_files_to_process',
        python_callable=check_for_files_to_process,
        provide_context=True,
        dag=dag)

# wraps the article title and abstract text with CDATA fields - this is
# necessary so that the XML parser doesn't fail when it encounters formatting
# elements, such as <b>, <i>, <sup>, etc., in the titles and abstracts.
wrap_with_cdata = PythonOperator(
        task_id='wrap_with_cdata',
        python_callable=wrap_title_and_abstract_with_cdata,
        provide_context=True,
        op_kwargs={'dir': '/home/airflow/gcs/data/pub_metadata_2023/to_load'},
        dag=dag)

# create the publication metadata files
create_publication_metadata = BashOperator(
    task_id='create_publication_metadata',
    bash_command="mkdir -p /home/airflow/gcs/data/pub_metadata_2023/metadata && ls -lhrt /home/airflow/gcs/data && jar -tf /home/airflow/gcs/data/$TM_PIPELINES_JAR_NAME | grep MedlineUiMetadataExtractor && java -cp /home/airflow/gcs/data/$TM_PIPELINES_JAR_NAME edu.cuanschutz.ccp.tm_provider.corpora.MedlineUiMetadataExtractor /home/airflow/gcs/data/pub_metadata_2023/to_load /home/airflow/gcs/data/pub_metadata_2023/metadata 0",
    env={'TM_PIPELINES_JAR_NAME': TM_PIPELINES_JAR_NAME},
    dag=dag)

# cat all publication metadata into a single file
aggregate_publication_metadata = BashOperator(
    task_id='aggregate_publication_metadata',
    bash_command="gunzip -c /home/airflow/gcs/data/pub_metadata_2023/metadata/*.ui_metadata.tsv.gz | grep DOC_ID | sort | uniq > /home/airflow/gcs/data/pub_metadata_2023/metadata/{{ ts_nodash }}.ui_metadata.tsv && gunzip -c /home/airflow/gcs/data/pub_metadata_2023/metadata/*.ui_metadata.tsv.gz | grep PMID >> /home/airflow/gcs/data/pub_metadata_2023/metadata/{{ ts_nodash }}.ui_metadata.tsv && gzip /home/airflow/gcs/data/pub_metadata_2023/metadata/{{ ts_nodash }}.ui_metadata.tsv && gunzip -c /home/airflow/gcs/data/pub_metadata_2023/metadata/*.ui_metadata.delete.tsv.gz > /home/airflow/gcs/data/pub_metadata_2023/metadata/{{ ts_nodash }}.ui_metadata.delete.tsv && gzip /home/airflow/gcs/data/pub_metadata_2023/metadata/{{ ts_nodash }}.ui_metadata.delete.tsv",
    dag=dag)

# copy the aggregate publication metadata file to the proper bucket
move_aggregate_publication_data_to_bucket = BashOperator(
    task_id='move_aggregate_publication_data_to_bucket',
    bash_command="CLOUDSDK_PYTHON=/usr/bin/python gsutil cp /home/airflow/gcs/data/pub_metadata_2023/metadata/{{ ts_nodash }}.ui_metadata.tsv.gz gs://$PUBLICATION_METADATA_BUCKET/$PUBLICATION_METADATA_PATH && CLOUDSDK_PYTHON=/usr/bin/python gsutil cp /home/airflow/gcs/data/pub_metadata_2023/metadata/{{ ts_nodash }}.ui_metadata.delete.tsv.gz gs://$PUBLICATION_METADATA_BUCKET/$PUBLICATION_METADATA_PATH",
    env={'PUBLICATION_METADATA_BUCKET': PUBLICATION_METADATA_BUCKET,
     'PUBLICATION_METADATA_PATH':PUBLICATION_METADATA_PATH},
    dag=dag)

def build_curl_cmd(filename):
    cmd = f'curl --location --request GET {PUBLICATION_METADATA_API_URL} --header "Content-Type: application/json" --data-raw \'{{ "source": {{ "bucket": "{PUBLICATION_METADATA_BUCKET}", "filepath": "{PUBLICATION_METADATA_PATH}/{filename}", "hmac_key_id": "{PUBLICATION_METADATA_SERVICE_HMAC_KEY_ID}", "hmac_secret": "{PUBLICATION_METADATA_SERVICE_HMAC_SECRET}" }} }}\''
    print(f'CURLCMD: {cmd}')
    return cmd

# invoke the Publication Metadata API to load the metadata
trigger_publication_metadata_load = BashOperator(
    task_id='trigger_publication_metadata_load',
    bash_command=build_curl_cmd("{{ ts_nodash }}.ui_metadata.tsv.gz"),
    dag=dag)

# invoke the Publication Metadata API to delete medline records
trigger_publication_metadata_delete = BashOperator(
    task_id='trigger_publication_metadata_delete',
    bash_command=build_curl_cmd("{{ ts_nodash }}.ui_metadata.delete.tsv.gz"),
    dag=dag)


# If all upstream tasks succeed, then remove files from the to_load directory.
# If any of the upstream processes failed then the files will be kept in the
# to_load directory so that they are processed the next time the workflow runs.
pipeline_end = BashOperator(
    task_id='pipeline_end',
    bash_command="rm /home/airflow/gcs/data/pub_metadata_2023/to_load/* && rm /home/airflow/gcs/data/pub_metadata_2023/metadata/*",
    dag=dag)


verify_md5.set_upstream(download)
populate_load_directory.set_upstream(verify_md5)
check_for_files_to_process.set_upstream(populate_load_directory)
wrap_with_cdata.set_upstream(check_for_files_to_process)
create_publication_metadata.set_upstream(wrap_with_cdata)
aggregate_publication_metadata.set_upstream(create_publication_metadata)
move_aggregate_publication_data_to_bucket.set_upstream(aggregate_publication_metadata)
trigger_publication_metadata_load.set_upstream(move_aggregate_publication_data_to_bucket)
trigger_publication_metadata_delete.set_upstream(trigger_publication_metadata_load)
pipeline_end.set_upstream(trigger_publication_metadata_delete)
pipeline_end.set_upstream(check_for_files_to_process)




