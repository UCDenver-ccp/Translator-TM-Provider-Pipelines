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
import re

from medline_xml_util import wrap_title_and_abstract_with_cdata


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

TM_PIPELINES_JAR=os.environ.get('TM_PIPELINES_JAR')
TM_PIPELINES_JAR_NAME=os.environ.get('TM_PIPELINES_JAR_NAME')

# PUBLICATION_METADATA_BUCKET=os.environ.get('PUBLICATION_METADATA_BUCKET')
# PUBLICATION_METADATA_PATH=os.environ.get('PUBLICATION_METADATA_PATH')

# PUBLICATION_METADATA_API_URL=os.environ.get('PUBLICATION_METADATA_API_URL')
# PUBLICATION_METADATA_SERVICE_HMAC_KEY_ID=os.environ.get('PUBLICATION_METADATA_SERVICE_HMAC_KEY_ID')
# PUBLICATION_METADATA_SERVICE_HMAC_SECRET=os.environ.get('PUBLICATION_METADATA_SERVICE_HMAC_SECRET')

# IDENTIFIER_COLLECTION_FILE_PREFIX=os.environ.get('PMID_COLLECTION_FILE_PREFIX')
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
    bash_command="cd /home/airflow/gcs/data/medline_load/update_files && wget -N 'ftp://ftp.ncbi.nlm.nih.gov:21/pubmed/updatefiles/pubmed23n*' 2>&1  | grep done | grep '.gz' | grep -v '.gz.md5' | tr -s ' ' | cut -f 7 -d ' '  > downloaded-files.txt",
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

# def wrap_title_and_abstract_with_cdata():
#     """Process each file in the to_load directory by creating an updated 
#     version whereby the abstract text and article title text has been wrapped
#     in a CDATA field. This is necessary b/c there are HTML fields, e.g. <b>, 
#     <i> in these fields and they cause the XML parser to break."""
#     for root, dirs, files in os.walk(os.path.abspath("/home/airflow/gcs/data/medline_load/to_load")):
#         for f in files:
#             fpath = os.path.join(root, f)
#             print(f'Wrapping CDATA for file: {fpath}')
#             tmpf = f'{fpath}.cdata'
#             with gzip.open(tmpf, mode="wt", encoding="utf-8") as out_file:
#                 with gzip.open(fpath, mode="rt", encoding="utf-8") as file:
#                     for line in file:
#                         updated_line = wrap_text_with_cdata(line)
#                         out_file.write(updated_line)
#             # copy/rename the temp file back to original file name
#             os.rename(tmpf, fpath)
#     return 'zip_to_load'


# def wrap_text_with_cdata(line):
#     """Use regular expressions to wrap the article title and abstract text 
#     fields with CDATA - this is necessary b/c there are HTML fields, e.g. <b>,
#     <i> in these fields and they cause the XML parser to break."""
#     updated_line = line
#     is_empty_article_title = re.match(r"\s*<ArticleTitle([^>]*)/>", updated_line)
#     if not is_empty_article_title:
#         p = re.compile(r"<ArticleTitle([^>]*)>")
#         updated_line = p.sub(r'<ArticleTitle\1><![CDATA[', updated_line)
#     p = re.compile(r"</ArticleTitle>")
#     updated_line = p.sub(r']]></ArticleTitle>', updated_line)
#     # note that there are instances of blank/empty abstract text fields, 
#     # e.g. <AbstractText Label="REVIEWERS' CONCLUSIONS" NlmCategory="CONCLUSIONS"/>. 
#     # For these, we do not want to add the CDATA tag so we check for these empty fields 
#     # and skip the CDATA wrapping if found.
#     is_empty_abstract_text = re.match(r"\s*<AbstractText([^>]*)/>", updated_line)
#     if not is_empty_abstract_text:
#         p = re.compile(r"<AbstractText([^>]*)>")
#         updated_line = p.sub(r'<AbstractText\1><![CDATA[', updated_line)
#     p = re.compile(r"</AbstractText>")
#     updated_line = p.sub(r']]></AbstractText>', updated_line)
#     return updated_line

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
        dag=dag)

# checks to see if the to_load directory is empty
def check_for_files_to_process(**kwargs):
    dir = os.listdir("/home/airflow/gcs/data/medline_load/to_load")
    if len(dir) > 0:
        return 'wrap_with_cdata'
        # return 'dataflow_medline_xml_to_text'
    return 'pipeline_end'

# checks to see if the to_load/ directory is empty. If not empty,
# then returns the id for the create_publication_metadata task.
check_for_files_to_process_op = BranchPythonOperator(
        task_id='check_for_files_to_process',
        python_callable=check_for_files_to_process,
        provide_context=True,
        dag=dag)

# # create the publication metadata files
# create_publication_metadata = BashOperator(
#     task_id='create_publication_metadata',
#     bash_command="mkdir -p /home/airflow/gcs/data/medline_load/metadata && ls -lhrt /home/airflow/gcs/data && jar -tf /home/airflow/gcs/data/$TM_PIPELINES_JAR_NAME | grep MedlineUiMetadataExtractor && java -cp /home/airflow/gcs/data/$TM_PIPELINES_JAR_NAME edu.cuanschutz.ccp.tm_provider.corpora.MedlineUiMetadataExtractor /home/airflow/gcs/data/medline_load/to_load /home/airflow/gcs/data/medline_load/metadata 0",
#     env={'TM_PIPELINES_JAR_NAME': TM_PIPELINES_JAR_NAME},
#     dag=dag)

# # cat all publication metadata into a single file
# aggregate_publication_metadata = BashOperator(
#     task_id='aggregate_publication_metadata',
#     bash_command="gunzip -c /home/airflow/gcs/data/medline_load/metadata/*.ui_metadata.tsv.gz | grep DOC_ID | sort | uniq > /home/airflow/gcs/data/medline_load/metadata/{{ ts_nodash }}.ui_metadata.tsv && gunzip -c /home/airflow/gcs/data/medline_load/metadata/*.ui_metadata.tsv.gz | grep PMID >> /home/airflow/gcs/data/medline_load/metadata/{{ ts_nodash }}.ui_metadata.tsv && gzip /home/airflow/gcs/data/medline_load/metadata/{{ ts_nodash }}.ui_metadata.tsv && gunzip -c /home/airflow/gcs/data/medline_load/metadata/*.ui_metadata.delete.tsv.gz > /home/airflow/gcs/data/medline_load/metadata/{{ ts_nodash }}.ui_metadata.delete.tsv && gzip /home/airflow/gcs/data/medline_load/metadata/{{ ts_nodash }}.ui_metadata.delete.tsv",
#     dag=dag)

# # create a file containing the PMIDs to add -- this will be used to update the file-based catalog of PMIDs that have been processed
# create_processed_pmids_file = BashOperator(
#     task_id='create_processed_pmids_file',
#     bash_command="gunzip -c /home/airflow/gcs/data/medline_load/metadata/{{ ts_nodash }}.ui_metadata.tsv.gz | cut -f 1 | grep PMID > /home/airflow/gcs/data/medline_load/metadata/processed.pmids",
#     dag=dag)

# # copy the aggregate publication metadata file to the proper bucket
# move_aggregate_publication_data_to_bucket = BashOperator(
#     task_id='move_aggregate_publication_data_to_bucket',
#     bash_command="CLOUDSDK_PYTHON=/usr/bin/python gsutil cp /home/airflow/gcs/data/medline_load/metadata/{{ ts_nodash }}.ui_metadata.tsv.gz gs://$PUBLICATION_METADATA_BUCKET/$PUBLICATION_METADATA_PATH && CLOUDSDK_PYTHON=/usr/bin/python gsutil cp /home/airflow/gcs/data/medline_load/metadata/{{ ts_nodash }}.ui_metadata.delete.tsv.gz gs://$PUBLICATION_METADATA_BUCKET/$PUBLICATION_METADATA_PATH",
#     env={'PUBLICATION_METADATA_BUCKET': PUBLICATION_METADATA_BUCKET,
#      'PUBLICATION_METADATA_PATH':PUBLICATION_METADATA_PATH},
#     dag=dag)

# def build_curl_cmd(filename):
#     cmd = f'curl --location --request GET {PUBLICATION_METADATA_API_URL} --header "Content-Type: application/json" --data-raw \'{{ "source": {{ "bucket": "{PUBLICATION_METADATA_BUCKET}", "filepath": "{PUBLICATION_METADATA_PATH}/{filename}", "hmac_key_id": "{PUBLICATION_METADATA_SERVICE_HMAC_KEY_ID}", "hmac_secret": "{PUBLICATION_METADATA_SERVICE_HMAC_SECRET}" }} }}\''
#     print(f'CURLCMD: {cmd}')
#     return cmd

# # invoke the Publication Metadata API to load the metadata
# trigger_publication_metadata_load = BashOperator(
#     task_id='trigger_publication_metadata_load',
#     bash_command=build_curl_cmd("{{ ts_nodash }}.ui_metadata.tsv.gz"),
#     dag=dag)

# # invoke the Publication Metadata API to delete medline records
# trigger_publication_metadata_delete = BashOperator(
#     task_id='trigger_publication_metadata_delete',
#     bash_command=build_curl_cmd("{{ ts_nodash }}.ui_metadata.delete.tsv.gz"),
#     dag=dag)

# call dataflow medline-xml-to-text to load the Medline record title/abstract into Cloud Datastore
dataflow_medline_xml_to_text_op = DataflowCreateJavaJobOperator(
    task_id="dataflow_medline_xml_to_text",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'medlineXmlDir': MEDLINE_XML_DIR,
        'collection': 'PUBMED',
        'pmidSkipFilePath': IDENTIFIER_COLLECTION_FILE_PREFIX + "*",
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


# # call dataflow to update the file-based catalog of PMIDs that have been processed. The input file for this
# # dataflow run is created above by the create_processed_pmids_file operator
# update_pubmed_ids_catalog = DataflowCreateJavaJobOperator(
#     task_id="update_pubmed_ids_catalog",
#     jar=TM_PIPELINES_JAR,
#     job_name='{{task.task_id}}',
#     options={
#         'collectionFilePrefix': IDENTIFIER_COLLECTION_FILE_PREFIX,
#         'inputFilePattern': COMPOSER_BUCKET_DATA_PATH + "/medline_load/metadata/processed.pmids",
#         'dateStamp': '{{ ts_nodash }}',
#         'numWorkers': 10,
#         'workerMachineType': 'n1-highmem-16'
#     },
#     poll_sleep=10,
#     job_class='edu.cuanschutz.ccp.tm_provider.etl.UpdateIdCollectionPipeline',
#     check_if_running=CheckJobRunning.IgnoreJob,
#     location=DATAFLOW_REGION,
#     retries=0,
#     dag=dag
# )


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
# create_publication_metadata.set_upstream(wrap_with_cdata)
# aggregate_publication_metadata.set_upstream(create_publication_metadata)
# move_aggregate_publication_data_to_bucket.set_upstream(aggregate_publication_metadata)
# create_processed_pmids_file.set_upstream(move_aggregate_publication_data_to_bucket)
# trigger_publication_metadata_load.set_upstream(create_processed_pmids_file)
# trigger_publication_metadata_delete.set_upstream(trigger_publication_metadata_load)
dataflow_medline_xml_to_text_op.set_upstream(wrap_with_cdata_op)
# update_pubmed_ids_catalog.set_upstream(dataflow_medline_xml_to_text)
pipeline_end_op.set_upstream(dataflow_medline_xml_to_text_op)
pipeline_end_op.set_upstream(check_for_files_to_process_op)




