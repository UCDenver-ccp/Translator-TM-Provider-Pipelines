from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator  import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import DAG
from airflow.utils import dates
from datetime import datetime, timedelta
from airflow.operators.sensors import ExternalTaskSensor

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


#====ENVIRONMENT VARIABLES THAT MUST BE SET IN CLOUD COMPOSER====
DATAFLOW_TMP_LOCATION=os.environ.get('DATAFLOW_TMP_LOCATION')
DATAFLOW_STAGING_LOCATION=os.environ.get('DATAFLOW_STAGING_LOCATION')
DATAFLOW_ZONE=os.environ.get('DATAFLOW_ZONE')
DATAFLOW_REGION=os.environ.get('DATAFLOW_REGION')

## this is the bucket that contains the ontology resources files
## this bucket also serves as a storage spot for intermediate output
RESOURCES_BUCKET=os.environ.get('RESOURCES_BUCKET')

PROJECT_ID=os.environ.get('GCP_PROJECT')

# TM_PIPELINES_JAR = the path to the tm-pipelines jar file (in some bucket)
TM_PIPELINES_JAR=os.environ.get('TM_PIPELINES_JAR')

# stage
CHEBI_OGER_SERVICE_URL=os.environ.get('CHEBI_OGER_SERVICE_URL')
CHEBI_CRF_SERVICE_URL=os.environ.get('CHEBI_CRF_SERVICE_URL')
CL_OGER_SERVICE_URL=os.environ.get('CL_OGER_SERVICE_URL')
CL_CRF_SERVICE_URL=os.environ.get('CL_CRF_SERVICE_URL')
DRUGBANK_OGER_SERVICE_URL=os.environ.get('DRUGBANK_OGER_SERVICE_URL')
GO_BP_OGER_SERVICE_URL=os.environ.get('GO_BP_OGER_SERVICE_URL')
GO_BP_CRF_SERVICE_URL=os.environ.get('GO_BP_CRF_SERVICE_URL')
GO_CC_OGER_SERVICE_URL=os.environ.get('GO_CC_OGER_SERVICE_URL')
GO_CC_CRF_SERVICE_URL=os.environ.get('GO_CC_CRF_SERVICE_URL')
GO_MF_OGER_SERVICE_URL=os.environ.get('GO_MF_OGER_SERVICE_URL')
GO_MF_CRF_SERVICE_URL=os.environ.get('GO_MF_CRF_SERVICE_URL')
HP_OGER_SERVICE_URL=os.environ.get('HP_OGER_SERVICE_URL')
HP_CRF_SERVICE_URL=os.environ.get('HP_CRF_SERVICE_URL')
MONDO_OGER_SERVICE_URL=os.environ.get('MONDO_OGER_SERVICE_URL')
MONDO_CRF_SERVICE_URL=os.environ.get('MONDO_CRF_SERVICE_URL')
MOP_OGER_SERVICE_URL=os.environ.get('MOP_OGER_SERVICE_URL')
MOP_CRF_SERVICE_URL=os.environ.get('MOP_CRF_SERVICE_URL')
NCBITAXON_OGER_SERVICE_URL=os.environ.get('NCBITAXON_OGER_SERVICE_URL')
NCBITAXON_CRF_SERVICE_URL=os.environ.get('NCBITAXON_CRF_SERVICE_URL')
PR_OGER_SERVICE_URL=os.environ.get('PR_OGER_SERVICE_URL')
PR_CRF_SERVICE_URL=os.environ.get('PR_CRF_SERVICE_URL')
SO_OGER_SERVICE_URL=os.environ.get('SO_OGER_SERVICE_URL')
SO_CRF_SERVICE_URL=os.environ.get('SO_CRF_SERVICE_URL')
UBERON_OGER_SERVICE_URL=os.environ.get('UBERON_OGER_SERVICE_URL')
UBERON_CRF_SERVICE_URL=os.environ.get('UBERON_CRF_SERVICE_URL')

CLOUD_SQL_DATABASE_NAME=os.environ.get('CLOUD_SQL_DATABASE_NAME')
CLOUD_SQL_DATABASE_USER=os.environ.get('CLOUD_SQL_DATABASE_USER')
CLOUD_SQL_DATABASE_PASSWORD=os.environ.get('CLOUD_SQL_DATABASE_PASSWORD')
CLOUD_SQL_INSTANCE_NAME=os.environ.get('CLOUD_SQL_INSTANCE_NAME')
CLOUD_SQL_REGION=os.environ.get('CLOUD_SQL_REGION')


INPUT_PIPELINE_KEY='MEDLINE_XML_TO_TEXT'
INPUT_PIPELINE_VERSION='0.1.0'

BERT_SCORE_INCLUSION_THRESHOLD = 0.9

# Note: "{{ ds }}" is the default Airflow date-stamp: YYYY-MM-DD. 
#        This same date-stamp format is also used when assigning 
#        date-stamp collection names in 
#        edu.cuanschutz.ccp.tm_provider.etl.fn.ToEntityFnUtils. 
#        These must remain in alignment!
#
# This function checks the dag_run.conf for a collection. Use it if it exists, otherwise use today's datestamp
# to override the collection, use the CLI, e.g. --conf '{"collection": "2021-08-23"}'
def get_collection():
    return "{{ dag_run.conf['collection'] if dag_run.conf['collection'] else ds }}"

COLLECTION=get_collection()


#==================DAG ARGUMENTS==============================

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 24),
    # run this dag at 2:30am MT every night which is 8:30am UTC
    # note that the document download script runs at midnight
    'schedule_interval': '30 8 * * *',
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'dataflow_default_options': {
        'zone': DATAFLOW_ZONE,
        'region': DATAFLOW_REGION,
        'stagingLocation': DATAFLOW_STAGING_LOCATION,
        'gcpTempLocation': DATAFLOW_TMP_LOCATION,
    }
}

dag = DAG(dag_id='process-medline-xml', default_args=args, catchup=False, schedule_interval='30 8 * * *')


# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
#                           SENTENCE SEGMENTATION
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================

## call dataflow to process medline xml for sentences
dataflow_medline_xml_sentences = DataflowCreateJavaJobOperator(
    task_id="dataflow_medline_xml_sentences",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'inputPipelineKey': INPUT_PIPELINE_KEY,
        'inputPipelineVersion': INPUT_PIPELINE_VERSION,
        'collection': COLLECTION,
        'overwrite': 'NO',
        'numWorkers': 25,
        'workerMachineType': 'n1-highmem-2'
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.SentenceSegmentationPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)

# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
#                       CONCEPT RECOGNITION: OGER
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================

# ----------- #
#    CHEBI    #
# ----------- #

# prime the CHEBI OGER service by calling it once
prime_chebi_oger = BashOperator(
    task_id='prime_chebi_oger',
    #bash_command="gcloud info",
    bash_command="curl -H \"Authorization: Bearer $(gcloud auth print-identity-token)\" -d \"Chlorine is a chemical.\" $CHEBI_OGER_SERVICE_URL/upload/txt/tsv/12345",
    env={'CHEBI_OGER_SERVICE_URL': CHEBI_OGER_SERVICE_URL},
    dag=dag)

## call dataflow to run the CHEBI OGER pipeline
chebi_oger = DataflowCreateJavaJobOperator(
    task_id="chebi_oger",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'ogerServiceUri': CHEBI_OGER_SERVICE_URL,
        'ogerOutputType': 'TSV',
        'targetProcessingStatusFlag': 'OGER_CHEBI_DONE',
        'targetDocumentType': 'CONCEPT_CHEBI',
        'targetDocumentFormat': 'BIONLP',
        'inputPipelineKey': INPUT_PIPELINE_KEY,
        'inputPipelineVersion': INPUT_PIPELINE_VERSION,
        'collection': COLLECTION,
        'overwrite': 'NO',
        'numWorkers': 25
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.OgerPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)

# ----------- #
#    CL    #
# ----------- #

# prime the CL OGER service by calling it once
prime_cl_oger = BashOperator(
    task_id='prime_cl_oger',
    #bash_command="gcloud info",
    bash_command="curl -H \"Authorization: Bearer $(gcloud auth print-identity-token)\" -d \"Chlorine is a chemical.\" $CL_OGER_SERVICE_URL/upload/txt/tsv/12345",
    env={'CL_OGER_SERVICE_URL': CL_OGER_SERVICE_URL},
    dag=dag)

## call dataflow to run the CL OGER pipeline
cl_oger = DataflowCreateJavaJobOperator(
    task_id="cl_oger",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'ogerServiceUri': CL_OGER_SERVICE_URL,
        'ogerOutputType': 'TSV',
        'targetProcessingStatusFlag': 'OGER_CL_DONE',
        'targetDocumentType': 'CONCEPT_CL',
        'targetDocumentFormat': 'BIONLP',
        'inputPipelineKey': INPUT_PIPELINE_KEY,
        'inputPipelineVersion': INPUT_PIPELINE_VERSION,
        'collection': COLLECTION,
        'overwrite': 'NO',
        'numWorkers': 25
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.OgerPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)


# -------------- #
#    DRUGBANK    #
# -------------- #

# prime the DRUGBANK OGER service by calling it once
prime_drugbank_oger = BashOperator(
    task_id='prime_drugbank_oger',
    #bash_command="gcloud info",
    bash_command="curl -H \"Authorization: Bearer $(gcloud auth print-identity-token)\" -d \"Metformin is a drug.\" $DRUGBANK_OGER_SERVICE_URL/upload/txt/tsv/12345",
    env={'DRUGBANK_OGER_SERVICE_URL': DRUGBANK_OGER_SERVICE_URL},
    dag=dag)

## call dataflow to run the CL OGER pipeline
drugbank_oger = DataflowCreateJavaJobOperator(
    task_id="drugbank_oger",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'ogerServiceUri': DRUGBANK_OGER_SERVICE_URL,
        'ogerOutputType': 'TSV',
        'targetProcessingStatusFlag': 'OGER_DRUGBANK_DONE',
        'targetDocumentType': 'CONCEPT_DRUGBANK',
        'targetDocumentFormat': 'BIONLP',
        'inputPipelineKey': INPUT_PIPELINE_KEY,
        'inputPipelineVersion': INPUT_PIPELINE_VERSION,
        'collection': COLLECTION,
        'overwrite': 'NO',
        'numWorkers': 25
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.OgerPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)

# ----------- #
#    GO_BP    #
# ----------- #

# prime the GO_BP OGER service by calling it once
prime_go_bp_oger = BashOperator(
    task_id='prime_go_bp_oger',
    #bash_command="gcloud info",
    bash_command="curl -H \"Authorization: Bearer $(gcloud auth print-identity-token)\" -d \"Chlorine is a chemical.\" $GO_BP_OGER_SERVICE_URL/upload/txt/tsv/12345",
    env={'GO_BP_OGER_SERVICE_URL': GO_BP_OGER_SERVICE_URL},
    dag=dag)

## call dataflow to run the GO_BP OGER pipeline
go_bp_oger = DataflowCreateJavaJobOperator(
    task_id="go_bp_oger",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'ogerServiceUri': GO_BP_OGER_SERVICE_URL,
        'ogerOutputType': 'TSV',
        'targetProcessingStatusFlag': 'OGER_GO_BP_DONE',
        'targetDocumentType': 'CONCEPT_GO_BP',
        'targetDocumentFormat': 'BIONLP',
        'inputPipelineKey': INPUT_PIPELINE_KEY,
        'inputPipelineVersion': INPUT_PIPELINE_VERSION,
        'collection': COLLECTION,
        'overwrite': 'NO',
        'numWorkers': 25
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.OgerPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)

# ----------- #
#    GO_CC    #
# ----------- #

# prime the GO_CC OGER service by calling it once
prime_go_cc_oger = BashOperator(
    task_id='prime_go_cc_oger',
    #bash_command="gcloud info",
    bash_command="curl -H \"Authorization: Bearer $(gcloud auth print-identity-token)\" -d \"Chlorine is a chemical.\" $GO_CC_OGER_SERVICE_URL/upload/txt/tsv/12345",
    env={'GO_CC_OGER_SERVICE_URL': GO_CC_OGER_SERVICE_URL},
    dag=dag)

## call dataflow to run the GO_CC OGER pipeline
go_cc_oger = DataflowCreateJavaJobOperator(
    task_id="go_cc_oger",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'ogerServiceUri': GO_CC_OGER_SERVICE_URL,
        'ogerOutputType': 'TSV',
        'targetProcessingStatusFlag': 'OGER_GO_CC_DONE',
        'targetDocumentType': 'CONCEPT_GO_CC',
        'targetDocumentFormat': 'BIONLP',
        'inputPipelineKey': INPUT_PIPELINE_KEY,
        'inputPipelineVersion': INPUT_PIPELINE_VERSION,
        'collection': COLLECTION,
        'overwrite': 'NO',
        'numWorkers': 25
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.OgerPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)

# ----------- #
#    GO_MF    #
# ----------- #

# prime the GO_MF OGER service by calling it once
prime_go_mf_oger = BashOperator(
    task_id='prime_go_mf_oger',
    #bash_command="gcloud info",
    bash_command="curl -H \"Authorization: Bearer $(gcloud auth print-identity-token)\" -d \"Chlorine is a chemical.\" $GO_MF_OGER_SERVICE_URL/upload/txt/tsv/12345",
    env={'GO_MF_OGER_SERVICE_URL': GO_MF_OGER_SERVICE_URL},
    dag=dag)

## call dataflow to run the GO_MF OGER pipeline
go_mf_oger = DataflowCreateJavaJobOperator(
    task_id="go_mf_oger",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'ogerServiceUri': GO_MF_OGER_SERVICE_URL,
        'ogerOutputType': 'TSV',
        'targetProcessingStatusFlag': 'OGER_GO_MF_DONE',
        'targetDocumentType': 'CONCEPT_GO_MF',
        'targetDocumentFormat': 'BIONLP',
        'inputPipelineKey': INPUT_PIPELINE_KEY,
        'inputPipelineVersion': INPUT_PIPELINE_VERSION,
        'collection': COLLECTION,
        'overwrite': 'NO',
        'numWorkers': 25
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.OgerPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)

# ----------- #
#    HP    #
# ----------- #

# prime the HP OGER service by calling it once
prime_hp_oger = BashOperator(
    task_id='prime_hp_oger',
    #bash_command="gcloud info",
    bash_command="curl -H \"Authorization: Bearer $(gcloud auth print-identity-token)\" -d \"Chlorine is a chemical.\" $HP_OGER_SERVICE_URL/upload/txt/tsv/12345",
    env={'HP_OGER_SERVICE_URL': HP_OGER_SERVICE_URL},
    dag=dag)

## call dataflow to run the HP OGER pipeline
hp_oger = DataflowCreateJavaJobOperator(
    task_id="hp_oger",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'ogerServiceUri': HP_OGER_SERVICE_URL,
        'ogerOutputType': 'TSV',
        'targetProcessingStatusFlag': 'OGER_HP_DONE',
        'targetDocumentType': 'CONCEPT_HP',
        'targetDocumentFormat': 'BIONLP',
        'inputPipelineKey': INPUT_PIPELINE_KEY,
        'inputPipelineVersion': INPUT_PIPELINE_VERSION,
        'collection': COLLECTION,
        'overwrite': 'NO',
        'numWorkers': 25
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.OgerPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)

# ----------- #
#    MONDO    #
# ----------- #

# prime the MONDO OGER service by calling it once
prime_mondo_oger = BashOperator(
    task_id='prime_mondo_oger',
    #bash_command="gcloud info",
    bash_command="curl -H \"Authorization: Bearer $(gcloud auth print-identity-token)\" -d \"Chlorine is a chemical.\" $MONDO_OGER_SERVICE_URL/upload/txt/tsv/12345",
    env={'MONDO_OGER_SERVICE_URL': MONDO_OGER_SERVICE_URL},
    dag=dag)

## call dataflow to run the MONDO OGER pipeline
mondo_oger = DataflowCreateJavaJobOperator(
    task_id="mondo_oger",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'ogerServiceUri': MONDO_OGER_SERVICE_URL,
        'ogerOutputType': 'TSV',
        'targetProcessingStatusFlag': 'OGER_MONDO_DONE',
        'targetDocumentType': 'CONCEPT_MONDO',
        'targetDocumentFormat': 'BIONLP',
        'inputPipelineKey': INPUT_PIPELINE_KEY,
        'inputPipelineVersion': INPUT_PIPELINE_VERSION,
        'collection': COLLECTION,
        'overwrite': 'NO',
        'numWorkers': 25
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.OgerPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)

# ----------- #
#    MOP    #
# ----------- #

# prime the MOP OGER service by calling it once
prime_mop_oger = BashOperator(
    task_id='prime_mop_oger',
    #bash_command="gcloud info",
    bash_command="curl -H \"Authorization: Bearer $(gcloud auth print-identity-token)\" -d \"Chlorine is a chemical.\" $MOP_OGER_SERVICE_URL/upload/txt/tsv/12345",
    env={'MOP_OGER_SERVICE_URL': MOP_OGER_SERVICE_URL},
    dag=dag)

## call dataflow to run the MOP OGER pipeline
mop_oger = DataflowCreateJavaJobOperator(
    task_id="mop_oger",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'ogerServiceUri': MOP_OGER_SERVICE_URL,
        'ogerOutputType': 'TSV',
        'targetProcessingStatusFlag': 'OGER_MOP_DONE',
        'targetDocumentType': 'CONCEPT_MOP',
        'targetDocumentFormat': 'BIONLP',
        'inputPipelineKey': INPUT_PIPELINE_KEY,
        'inputPipelineVersion': INPUT_PIPELINE_VERSION,
        'collection': COLLECTION,
        'overwrite': 'NO',
        'numWorkers': 25
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.OgerPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)

# ----------- #
#    NCBITAXON    #
# ----------- #

# prime the NCBITAXON OGER service by calling it once
prime_ncbitaxon_oger = BashOperator(
    task_id='prime_ncbitaxon_oger',
    #bash_command="gcloud info",
    bash_command="curl -H \"Authorization: Bearer $(gcloud auth print-identity-token)\" -d \"Chlorine is a chemical.\" $NCBITAXON_OGER_SERVICE_URL/upload/txt/tsv/12345",
    env={'NCBITAXON_OGER_SERVICE_URL': NCBITAXON_OGER_SERVICE_URL},
    dag=dag)

## call dataflow to run the NCBITAXON OGER pipeline
ncbitaxon_oger = DataflowCreateJavaJobOperator(
    task_id="ncbitaxon_oger",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'ogerServiceUri': NCBITAXON_OGER_SERVICE_URL,
        'ogerOutputType': 'TSV',
        'targetProcessingStatusFlag': 'OGER_NCBITAXON_DONE',
        'targetDocumentType': 'CONCEPT_NCBITAXON',
        'targetDocumentFormat': 'BIONLP',
        'inputPipelineKey': INPUT_PIPELINE_KEY,
        'inputPipelineVersion': INPUT_PIPELINE_VERSION,
        'collection': COLLECTION,
        'overwrite': 'NO',
        'numWorkers': 25
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.OgerPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)

# ----------- #
#      PR     #
# ----------- #

# prime the PR OGER service by calling it once
prime_pr_oger = BashOperator(
    task_id='prime_pr_oger',
    #bash_command="gcloud info",
    bash_command="curl -H \"Authorization: Bearer $(gcloud auth print-identity-token)\" -d \"Chlorine is a chemical.\" $PR_OGER_SERVICE_URL/upload/txt/tsv/12345",
    env={'PR_OGER_SERVICE_URL': PR_OGER_SERVICE_URL},
    dag=dag)

## call dataflow to run the PR OGER pipeline
pr_oger = DataflowCreateJavaJobOperator(
    task_id="pr_oger",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'ogerServiceUri': PR_OGER_SERVICE_URL,
        'ogerOutputType': 'TSV',
        'targetProcessingStatusFlag': 'OGER_PR_DONE',
        'targetDocumentType': 'CONCEPT_PR',
        'targetDocumentFormat': 'BIONLP',
        'inputPipelineKey': INPUT_PIPELINE_KEY,
        'inputPipelineVersion': INPUT_PIPELINE_VERSION,
        'collection': COLLECTION,
        'overwrite': 'NO',
        'numWorkers': 25
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.OgerPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)

# ----------- #
#    SO    #
# ----------- #

# prime the SO OGER service by calling it once
prime_so_oger = BashOperator(
    task_id='prime_so_oger',
    #bash_command="gcloud info",
    bash_command="curl -H \"Authorization: Bearer $(gcloud auth print-identity-token)\" -d \"Chlorine is a chemical.\" $SO_OGER_SERVICE_URL/upload/txt/tsv/12345",
    env={'SO_OGER_SERVICE_URL': SO_OGER_SERVICE_URL},
    dag=dag)

## call dataflow to run the SO OGER pipeline
so_oger = DataflowCreateJavaJobOperator(
    task_id="so_oger",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'ogerServiceUri': SO_OGER_SERVICE_URL,
        'ogerOutputType': 'TSV',
        'targetProcessingStatusFlag': 'OGER_SO_DONE',
        'targetDocumentType': 'CONCEPT_SO',
        'targetDocumentFormat': 'BIONLP',
        'inputPipelineKey': INPUT_PIPELINE_KEY,
        'inputPipelineVersion': INPUT_PIPELINE_VERSION,
        'collection': COLLECTION,
        'overwrite': 'NO',
        'numWorkers': 25
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.OgerPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)

# ----------- #
#    UBERON    #
# ----------- #

# prime the UBERON OGER service by calling it once
prime_uberon_oger = BashOperator(
    task_id='prime_uberon_oger',
    #bash_command="gcloud info",
    bash_command="curl -H \"Authorization: Bearer $(gcloud auth print-identity-token)\" -d \"Chlorine is a chemical.\" $UBERON_OGER_SERVICE_URL/upload/txt/tsv/12345",
    env={'UBERON_OGER_SERVICE_URL': UBERON_OGER_SERVICE_URL},
    dag=dag)

## call dataflow to run the UBERON OGER pipeline
uberon_oger = DataflowCreateJavaJobOperator(
    task_id="uberon_oger",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'ogerServiceUri': UBERON_OGER_SERVICE_URL,
        'ogerOutputType': 'TSV',
        'targetProcessingStatusFlag': 'OGER_UBERON_DONE',
        'targetDocumentType': 'CONCEPT_UBERON',
        'targetDocumentFormat': 'BIONLP',
        'inputPipelineKey': INPUT_PIPELINE_KEY,
        'inputPipelineVersion': INPUT_PIPELINE_VERSION,
        'collection': COLLECTION,
        'overwrite': 'NO',
        'numWorkers': 25
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.OgerPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)



# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
#                       CONCEPT RECOGNITION: CRF
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================

# ----------- #
#    CHEBI    #
# ----------- #

# prime the CHEBI CRF service by calling it once
prime_chebi_crf = BashOperator(
    task_id='prime_chebi_crf',
    #bash_command="gcloud info",
    bash_command="curl -H \"Authorization: Bearer $(gcloud auth print-identity-token)\" -d \"Chlorine is a chemical.\" $CHEBI_CRF_SERVICE_URL/crf",
    env={'CHEBI_CRF_SERVICE_URL': CHEBI_CRF_SERVICE_URL},
    dag=dag)

## call dataflow to run the CHEBI CRF pipeline
chebi_crf = DataflowCreateJavaJobOperator(
    task_id="chebi_crf",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'crfServiceUri': CHEBI_CRF_SERVICE_URL,
        'targetProcessingStatusFlag': 'CRF_CHEBI_DONE',
        'targetDocumentType': 'CRF_CHEBI',
        'inputSentencePipelineKey':'SENTENCE_SEGMENTATION',
        'inputSentencePipelineVersion':'0.1.0',
        'collection': COLLECTION,
        'overwrite': 'NO',
        'numWorkers': 25
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.CrfNerPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)


# ----------- #
#    CL    #
# ----------- #

# prime the CL CRF service by calling it once
prime_cl_crf = BashOperator(
    task_id='prime_cl_crf',
    #bash_command="gcloud info",
    bash_command="curl -H \"Authorization: Bearer $(gcloud auth print-identity-token)\" -d \"Chlorine is a chemical.\" $CL_CRF_SERVICE_URL/crf",
    env={'CL_CRF_SERVICE_URL': CL_CRF_SERVICE_URL},
    dag=dag)

## call dataflow to run the CL CRF pipeline
cl_crf = DataflowCreateJavaJobOperator(
    task_id="cl_crf",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'crfServiceUri': CL_CRF_SERVICE_URL,
        'targetProcessingStatusFlag': 'CRF_CL_DONE',
        'targetDocumentType': 'CRF_CL',
        'inputSentencePipelineKey':'SENTENCE_SEGMENTATION',
        'inputSentencePipelineVersion':'0.1.0',
        'collection': COLLECTION,
        'overwrite': 'NO',
        'numWorkers': 25
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.CrfNerPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)

# ----------- #
#    GO_BP    #
# ----------- #

# prime the GO_BP CRF service by calling it once
prime_go_bp_crf = BashOperator(
    task_id='prime_go_bp_crf',
    #bash_command="gcloud info",
    bash_command="curl -H \"Authorization: Bearer $(gcloud auth print-identity-token)\" -d \"Chlorine is a chemical.\" $GO_BP_CRF_SERVICE_URL/crf",
    env={'GO_BP_CRF_SERVICE_URL': GO_BP_CRF_SERVICE_URL},
    dag=dag)

## call dataflow to run the GO_BP CRF pipeline
go_bp_crf = DataflowCreateJavaJobOperator(
    task_id="go_bp_crf",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'crfServiceUri': GO_BP_CRF_SERVICE_URL,
        'targetProcessingStatusFlag': 'CRF_GO_BP_DONE',
        'targetDocumentType': 'CRF_GO_BP',
        'inputSentencePipelineKey':'SENTENCE_SEGMENTATION',
        'inputSentencePipelineVersion':'0.1.0',
        'collection': COLLECTION,
        'overwrite': 'NO',
        'numWorkers': 25
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.CrfNerPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)

# ----------- #
#    GO_CC    #
# ----------- #

# prime the GO_CC CRF service by calling it once
prime_go_cc_crf = BashOperator(
    task_id='prime_go_cc_crf',
    #bash_command="gcloud info",
    bash_command="curl -H \"Authorization: Bearer $(gcloud auth print-identity-token)\" -d \"Chlorine is a chemical.\" $GO_CC_CRF_SERVICE_URL/crf",
    env={'GO_CC_CRF_SERVICE_URL': GO_CC_CRF_SERVICE_URL},
    dag=dag)

## call dataflow to run the GO_CC CRF pipeline
go_cc_crf = DataflowCreateJavaJobOperator(
    task_id="go_cc_crf",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'crfServiceUri': GO_CC_CRF_SERVICE_URL,
        'targetProcessingStatusFlag': 'CRF_GO_CC_DONE',
        'targetDocumentType': 'CRF_GO_CC',
        'inputSentencePipelineKey':'SENTENCE_SEGMENTATION',
        'inputSentencePipelineVersion':'0.1.0',
        'collection': COLLECTION,
        'overwrite': 'NO',
        'numWorkers': 25
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.CrfNerPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)

# ----------- #
#    GO_MF    #
# ----------- #

# prime the GO_MF CRF service by calling it once
prime_go_mf_crf = BashOperator(
    task_id='prime_go_mf_crf',
    #bash_command="gcloud info",
    bash_command="curl -H \"Authorization: Bearer $(gcloud auth print-identity-token)\" -d \"Chlorine is a chemical.\" $GO_MF_CRF_SERVICE_URL/crf",
    env={'GO_MF_CRF_SERVICE_URL': GO_MF_CRF_SERVICE_URL},
    dag=dag)

## call dataflow to run the GO_MF CRF pipeline
go_mf_crf = DataflowCreateJavaJobOperator(
    task_id="go_mf_crf",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'crfServiceUri': GO_MF_CRF_SERVICE_URL,
        'targetProcessingStatusFlag': 'CRF_GO_MF_DONE',
        'targetDocumentType': 'CRF_GO_MF',
        'inputSentencePipelineKey':'SENTENCE_SEGMENTATION',
        'inputSentencePipelineVersion':'0.1.0',
        'collection': COLLECTION,
        'overwrite': 'NO',
        'numWorkers': 25
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.CrfNerPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)

# ----------- #
#    HP    #
# ----------- #

# prime the HP CRF service by calling it once
prime_hp_crf = BashOperator(
    task_id='prime_hp_crf',
    #bash_command="gcloud info",
    bash_command="curl -H \"Authorization: Bearer $(gcloud auth print-identity-token)\" -d \"Chlorine is a chemical.\" $HP_CRF_SERVICE_URL/crf",
    env={'HP_CRF_SERVICE_URL': HP_CRF_SERVICE_URL},
    dag=dag)

## call dataflow to run the HP CRF pipeline
hp_crf = DataflowCreateJavaJobOperator(
    task_id="hp_crf",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'crfServiceUri': HP_CRF_SERVICE_URL,
        'targetProcessingStatusFlag': 'CRF_HP_DONE',
        'targetDocumentType': 'CRF_HP',
        'inputSentencePipelineKey':'SENTENCE_SEGMENTATION',
        'inputSentencePipelineVersion':'0.1.0',
        'collection': COLLECTION,
        'overwrite': 'NO',
        'numWorkers': 25
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.CrfNerPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)

# ----------- #
#    MONDO    #
# ----------- #

# prime the MONDO CRF service by calling it once
prime_mondo_crf = BashOperator(
    task_id='prime_mondo_crf',
    #bash_command="gcloud info",
    bash_command="curl -H \"Authorization: Bearer $(gcloud auth print-identity-token)\" -d \"Chlorine is a chemical.\" $MONDO_CRF_SERVICE_URL/crf",
    env={'MONDO_CRF_SERVICE_URL': MONDO_CRF_SERVICE_URL},
    dag=dag)

## call dataflow to run the MONDO CRF pipeline
mondo_crf = DataflowCreateJavaJobOperator(
    task_id="mondo_crf",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'crfServiceUri': MONDO_CRF_SERVICE_URL,
        'targetProcessingStatusFlag': 'CRF_MONDO_DONE',
        'targetDocumentType': 'CRF_MONDO',
        'inputSentencePipelineKey':'SENTENCE_SEGMENTATION',
        'inputSentencePipelineVersion':'0.1.0',
        'collection': COLLECTION,
        'overwrite': 'NO',
        'numWorkers': 25
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.CrfNerPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)

# ----------- #
#    MOP    #
# ----------- #

# prime the MOP CRF service by calling it once
prime_mop_crf = BashOperator(
    task_id='prime_mop_crf',
    #bash_command="gcloud info",
    bash_command="curl -H \"Authorization: Bearer $(gcloud auth print-identity-token)\" -d \"Chlorine is a chemical.\" $MOP_CRF_SERVICE_URL/crf",
    env={'MOP_CRF_SERVICE_URL': MOP_CRF_SERVICE_URL},
    dag=dag)

## call dataflow to run the MOP CRF pipeline
mop_crf = DataflowCreateJavaJobOperator(
    task_id="mop_crf",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'crfServiceUri': MOP_CRF_SERVICE_URL,
        'targetProcessingStatusFlag': 'CRF_MOP_DONE',
        'targetDocumentType': 'CRF_MOP',
        'inputSentencePipelineKey':'SENTENCE_SEGMENTATION',
        'inputSentencePipelineVersion':'0.1.0',
        'collection': COLLECTION,
        'overwrite': 'NO',
        'numWorkers': 25
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.CrfNerPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)

# ----------- #
#    NCBITAXON    #
# ----------- #

# prime the NCBITAXON CRF service by calling it once
prime_ncbitaxon_crf = BashOperator(
    task_id='prime_ncbitaxon_crf',
    #bash_command="gcloud info",
    bash_command="curl -H \"Authorization: Bearer $(gcloud auth print-identity-token)\" -d \"Chlorine is a chemical.\" $NCBITAXON_CRF_SERVICE_URL/crf",
    env={'NCBITAXON_CRF_SERVICE_URL': NCBITAXON_CRF_SERVICE_URL},
    dag=dag)

## call dataflow to run the NCBITAXON CRF pipeline
ncbitaxon_crf = DataflowCreateJavaJobOperator(
    task_id="ncbitaxon_crf",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'crfServiceUri': NCBITAXON_CRF_SERVICE_URL,
        'targetProcessingStatusFlag': 'CRF_NCBITAXON_DONE',
        'targetDocumentType': 'CRF_NCBITAXON',
        'inputSentencePipelineKey':'SENTENCE_SEGMENTATION',
        'inputSentencePipelineVersion':'0.1.0',
        'collection': COLLECTION,
        'overwrite': 'NO',
        'numWorkers': 25
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.CrfNerPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)

# ----------- #
#      PR     #
# ----------- #

# prime the PR CRF service by calling it once
prime_pr_crf = BashOperator(
    task_id='prime_pr_crf',
    #bash_command="gcloud info",
    bash_command="curl -H \"Authorization: Bearer $(gcloud auth print-identity-token)\" -d \"Chlorine is a chemical.\" $PR_CRF_SERVICE_URL/crf",
    env={'PR_CRF_SERVICE_URL': PR_CRF_SERVICE_URL},
    dag=dag)

## call dataflow to run the PR CRF pipeline
pr_crf = DataflowCreateJavaJobOperator(
    task_id="pr_crf",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'crfServiceUri': PR_CRF_SERVICE_URL,
        'targetProcessingStatusFlag': 'CRF_PR_DONE',
        'targetDocumentType': 'CRF_PR',
        'inputSentencePipelineKey':'SENTENCE_SEGMENTATION',
        'inputSentencePipelineVersion':'0.1.0',
        'collection': COLLECTION,
        'overwrite': 'NO',
        'numWorkers': 25
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.CrfNerPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)

# ----------- #
#    SO    #
# ----------- #

# prime the SO CRF service by calling it once
prime_so_crf = BashOperator(
    task_id='prime_so_crf',
    #bash_command="gcloud info",
    bash_command="curl -H \"Authorization: Bearer $(gcloud auth print-identity-token)\" -d \"Chlorine is a chemical.\" $SO_CRF_SERVICE_URL/crf",
    env={'SO_CRF_SERVICE_URL': SO_CRF_SERVICE_URL},
    dag=dag)

## call dataflow to run the SO CRF pipeline
so_crf = DataflowCreateJavaJobOperator(
    task_id="so_crf",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'crfServiceUri': SO_CRF_SERVICE_URL,
        'targetProcessingStatusFlag': 'CRF_SO_DONE',
        'targetDocumentType': 'CRF_SO',
        'inputSentencePipelineKey':'SENTENCE_SEGMENTATION',
        'inputSentencePipelineVersion':'0.1.0',
        'collection': COLLECTION,
        'overwrite': 'NO',
        'numWorkers': 25
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.CrfNerPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)

# ----------- #
#    UBERON    #
# ----------- #

# prime the UBERON CRF service by calling it once
prime_uberon_crf = BashOperator(
    task_id='prime_uberon_crf',
    #bash_command="gcloud info",
    bash_command="curl -H \"Authorization: Bearer $(gcloud auth print-identity-token)\" -d \"Chlorine is a chemical.\" $UBERON_CRF_SERVICE_URL/crf",
    env={'UBERON_CRF_SERVICE_URL': UBERON_CRF_SERVICE_URL},
    dag=dag)

## call dataflow to run the UBERON CRF pipeline
uberon_crf = DataflowCreateJavaJobOperator(
    task_id="uberon_crf",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'crfServiceUri': UBERON_CRF_SERVICE_URL,
        'targetProcessingStatusFlag': 'CRF_UBERON_DONE',
        'targetDocumentType': 'CRF_UBERON',
        'inputSentencePipelineKey':'SENTENCE_SEGMENTATION',
        'inputSentencePipelineVersion':'0.1.0',
        'collection': COLLECTION,
        'overwrite': 'NO',
        'numWorkers': 25
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.CrfNerPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)




# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
#                   CONCEPT RECOGNITION: POST-PROCESSING
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================


PR_PROMOTION_MAP_FILE_PATH=RESOURCES_BUCKET + "/ontology-resources/pr-promotion-map.tsv.gz"
NCBITAXON_PROMOTION_MAP_FILE_PATH=RESOURCES_BUCKET + "/ontology-resources/ncbitaxon-promotion-map.tsv.gz"
EXTENSION_MAP_FILE_PATH=RESOURCES_BUCKET + "/ontology-resources/craft-mapping-files/*.txt.gz"

INPUT_DOC_CRITERIA="TEXT|TEXT|"+INPUT_PIPELINE_KEY+"|"+INPUT_PIPELINE_VERSION+";CONCEPT_DRUGBANK|BIONLP|OGER|0.1.0;CONCEPT_CHEBI|BIONLP|OGER|0.1.0;CRF_CHEBI|BIONLP|CRF|0.1.0;CONCEPT_PR|BIONLP|OGER|0.1.0;CRF_PR|BIONLP|CRF|0.1.0;CONCEPT_CL|BIONLP|OGER|0.1.0;CRF_CL|BIONLP|CRF|0.1.0;CONCEPT_UBERON|BIONLP|OGER|0.1.0;CRF_UBERON|BIONLP|CRF|0.1.0;CONCEPT_GO_BP|BIONLP|OGER|0.1.0;CRF_GO_BP|BIONLP|CRF|0.1.0;CONCEPT_GO_CC|BIONLP|OGER|0.1.0;CRF_GO_CC|BIONLP|CRF|0.1.0;CONCEPT_GO_MF|BIONLP|OGER|0.1.0;CRF_GO_MF|BIONLP|CRF|0.1.0;CONCEPT_SO|BIONLP|OGER|0.1.0;CRF_SO|BIONLP|CRF|0.1.0;CONCEPT_NCBITAXON|BIONLP|OGER|0.1.0;CRF_NCBITAXON|BIONLP|CRF|0.1.0;CONCEPT_HP|BIONLP|OGER|0.1.0;CRF_HP|BIONLP|CRF|0.1.0;CONCEPT_MONDO|BIONLP|OGER|0.1.0;CRF_MONDO|BIONLP|CRF|0.1.0;CONCEPT_MOP|BIONLP|OGER|0.1.0;CRF_MOP|BIONLP|CRF|0.1.0"

## call dataflow to run concept post-processing
concept_post_process = DataflowCreateJavaJobOperator(
    task_id="concept_post_process",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'inputDocumentCriteria': INPUT_DOC_CRITERIA,
        'requiredProcessingStatusFlags': 'TEXT_DONE',
        'prPromotionMapFilePath': PR_PROMOTION_MAP_FILE_PATH,
        'prPromotionMapFileDelimiter': 'TAB',
        'ncbiTaxonPromotionMapFilePath': NCBITAXON_PROMOTION_MAP_FILE_PATH,
        'ncbiTaxonPromotionMapFileDelimiter': 'TAB',
        'ncbiTaxonPromotionMapFileSetDelimiter': 'PIPE',
        'extensionMapFilePath': EXTENSION_MAP_FILE_PATH,
        'extensionMapFileDelimiter': 'TAB',
        'extensionMapFileSetDelimiter': 'TAB',
        'filterFlag': 'BY_CRF',
        'targetProcessingStatusFlag': 'CONCEPT_POST_PROCESSING_DONE',
        'collection': COLLECTION,
        'overwrite': 'NO',
        'numWorkers': 10
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.ConceptPostProcessingPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)

## _UNFILTERED refers to concept post-processing being done without filtering using the CRFs
INPUT_DOC_CRITERIA_UNFILTERED="TEXT|TEXT|"+INPUT_PIPELINE_KEY+"|"+INPUT_PIPELINE_VERSION+";CONCEPT_DRUGBANK|BIONLP|OGER|0.1.0;CONCEPT_CHEBI|BIONLP|OGER|0.1.0;CONCEPT_PR|BIONLP|OGER|0.1.0;CONCEPT_CL|BIONLP|OGER|0.1.0;CONCEPT_UBERON|BIONLP|OGER|0.1.0;CONCEPT_GO_BP|BIONLP|OGER|0.1.0;CONCEPT_GO_CC|BIONLP|OGER|0.1.0;CONCEPT_GO_MF|BIONLP|OGER|0.1.0;CONCEPT_SO|BIONLP|OGER|0.1.0;CONCEPT_NCBITAXON|BIONLP|OGER|0.1.0;CONCEPT_HP|BIONLP|OGER|0.1.0;CONCEPT_MONDO|BIONLP|OGER|0.1.0;CONCEPT_MOP|BIONLP|OGER|0.1.0"

## call dataflow to run concept post-processing with NO CRF FILTERING
concept_post_process_unfiltered = DataflowCreateJavaJobOperator(
    task_id="concept_post_process_unfiltered",
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'inputDocumentCriteria': INPUT_DOC_CRITERIA_UNFILTERED,
        'requiredProcessingStatusFlags': 'TEXT_DONE',
        'prPromotionMapFilePath': PR_PROMOTION_MAP_FILE_PATH,
        'prPromotionMapFileDelimiter': 'TAB',
        'ncbiTaxonPromotionMapFilePath': NCBITAXON_PROMOTION_MAP_FILE_PATH,
        'ncbiTaxonPromotionMapFileDelimiter': 'TAB',
        'ncbiTaxonPromotionMapFileSetDelimiter': 'PIPE',
        'extensionMapFilePath': EXTENSION_MAP_FILE_PATH,
        'extensionMapFileDelimiter': 'TAB',
        'extensionMapFileSetDelimiter': 'TAB',
        'filterFlag': 'NONE',
        'targetProcessingStatusFlag': 'CONCEPT_POST_PROCESSING_UNFILTERED_DONE',
        'collection': COLLECTION,
        'overwrite': 'NO',
        'numWorkers': 10
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.ConceptPostProcessingPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)


# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
#                             BL:CHEMICAL_TO_GENE
#         EXTRACT SENTENCES, CLASSIFY, STORE RESULTS IN CLOUD SQL DB
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================

ASSOCIATION_KEY_LC = "bl_chemical_to_gene"
ASSOCIATION_KEY_UC = ASSOCIATION_KEY_LC.upper()
PREFIX_X = "CHEBI"
PLACEHOLDER_X = "@CHEMICAL$"
PREFIX_Y = "PR"
PLACEHOLDER_Y = "@GENE$"

# name of the file containing sentences to be classified by the BERT model plus relevant sentence metadata that must accompany the classification results
BERT_INPUT_FILE_NAME_WITH_METADATA = 'bert-input-' + ASSOCIATION_KEY_LC + '.metadata.' + COLLECTION + '.tsv'

# name of the file containing all sentences that will be classified by the BERT model
BERT_INPUT_FILE_NAME = 'bert-input-' + ASSOCIATION_KEY_LC + '.' + COLLECTION + '.tsv'

# output bucket where a file containing relevant sentences to-be-classified will be stored
TO_BE_CLASSIFIED_SENTENCE_BUCKET = RESOURCES_BUCKET + '/output/sentences/' + ASSOCIATION_KEY_LC + '/' + COLLECTION

# the prefix for the files that contain the relevant to-be-classified sentences. 
# Data is processed in parallel so multiple files with this prefix will likely be created.
SENTENCE_FILE_PREFIX = TO_BE_CLASSIFIED_SENTENCE_BUCKET + '/' + ASSOCIATION_KEY_LC

# the bucket where the BERT input file will be copied after its creation
BERT_INPUT_SENTENCE_BUCKET = TO_BE_CLASSIFIED_SENTENCE_BUCKET + '/bert-input'

# Note that the BERT prediction containers will add an output path for the classified sentences, e.g.
# [BUCKET]/output/classified_sentences/bl_chemical_to_disease_or_phenotypic_feature/0.1.2

# clean up output sentence files - delete any sentences remaining from previous runs if any exist
delete_sentences_bl_chemical_to_gene_1 = BashOperator(
    task_id='delete_old_sentences_' + ASSOCIATION_KEY_LC + '_1',
    bash_command='gsutil -q stat "' + SENTENCE_FILE_PREFIX + '*"; if [ "$?" -eq "0" ]; then gsutil rm "' + SENTENCE_FILE_PREFIX + '*"; fi',
    dag=dag)

delete_sentences_bl_chemical_to_gene_2 = BashOperator(
    task_id='delete_old_sentences_' + ASSOCIATION_KEY_LC + '_2',
    bash_command='gsutil -q stat "' + SENTENCE_FILE_PREFIX + '*"; if [ "$?" -eq "0" ]; then gsutil rm "' + SENTENCE_FILE_PREFIX + '*"; fi',
    dag=dag)


## call dataflow to extract sentences with chemicals and proteins
## Note: proteins will be labeled as @GENE$ in the output
sentence_extraction_bl_chemical_to_gene = DataflowCreateJavaJobOperator(
    task_id='sentence_extraction_' + ASSOCIATION_KEY_LC,
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'targetProcessingStatusFlag': 'SENTENCE_DONE',
        'inputDocumentCriteria': "TEXT|TEXT|"+INPUT_PIPELINE_KEY+"|"+INPUT_PIPELINE_VERSION+";SECTIONS|BIONLP|"+INPUT_PIPELINE_KEY+"|"+INPUT_PIPELINE_VERSION +";SENTENCE|BIONLP|SENTENCE_SEGMENTATION|0.1.0;CONCEPT_ALL_UNFILTERED|BIONLP|CONCEPT_POST_PROCESS|0.1.0",
        'keywords':"",
        'outputBucket': SENTENCE_FILE_PREFIX,
        'prefixX': PREFIX_X,
        'placeholderX': PLACEHOLDER_X,
        'prefixY': PREFIX_Y,
        'placeholderY': PLACEHOLDER_Y,
        'collection': COLLECTION,
        'overwrite': 'YES',
        'numWorkers': 10
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.SentenceExtractionPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)

# cat all sentences into a single file that contains the sentences + metadata, 
# then take the first 2 columns from that file and create the file that will be 
# processed by BERT then 'gsutil cp' both files to the TO_BE_CLASSIFIED_SENTENCE_BUCKET
cat_sentences_bl_chemical_to_gene = BashOperator(
    task_id='cat_sentences_bl_chemical_to_gene',
    bash_command="mkdir -p /home/airflow/gcs/data/to_bert && cd /home/airflow/gcs/data/to_bert && gsutil cat " + SENTENCE_FILE_PREFIX + "* > " + BERT_INPUT_FILE_NAME_WITH_METADATA + " && gsutil cp " + BERT_INPUT_FILE_NAME_WITH_METADATA + " " + TO_BE_CLASSIFIED_SENTENCE_BUCKET + " && cut -f 1-2 " + BERT_INPUT_FILE_NAME_WITH_METADATA + " > " + BERT_INPUT_FILE_NAME + " && gsutil cp " + BERT_INPUT_FILE_NAME + " " + BERT_INPUT_SENTENCE_BUCKET + "/",
    dag=dag)

# =============================================================================
#                            SENTENCE CLASSIFICATION
# =============================================================================

# call ai-platform to classify the sentences created in the previous step
# Note: There doesn't seem to be an airflow operator for calling custom containers 
#       on AI Platform at this time so we'll use a BashOperator here and will then 
#       monitor the AI Platform job until it completes.

BERT_MODEL_VERSION = "0.1"
BASE_IMAGE_NAME = ASSOCIATION_KEY_LC
PREDICT_IMAGE_NAME = BASE_IMAGE_NAME + "-predict"

# Note that the BERT prediction containers will add an output path for the classified sentences, e.g.
# [RESOURCES_BUCKET]/output/classified_sentences/bl_chemical_to_disease_or_phenotypic_feature/0.1.2

# Note: NO_ARG is a required placeholder in the gcloud ai-platform call below. If not present, the first input argument ends up missing for some reason
classify_bl_chemical_to_gene_sentences = BashOperator(
    task_id='classify_' + ASSOCIATION_KEY_LC + '_sentences',
    bash_command="""
gcloud ai-platform jobs submit training "classify_${ASSOCIATION_KEY}_sentences_{{ ts_nodash }}" \
       --scale-tier basic_gpu --region "$DATAFLOW_REGION" \
       --master-image-uri "gcr.io/$PROJECT_ID/$IMAGE_NAME:$IMAGE_VERSION" \
       -- \
       "NO_ARG" \
       $SENTENCE_BUCKET \
       $COLLECTION \
       $CLASSIFIED_SENTENCE_BUCKET
""",
    env={'DATAFLOW_REGION' : DATAFLOW_REGION,
    'IMAGE_NAME' : PREDICT_IMAGE_NAME,
    'IMAGE_VERSION' : BERT_MODEL_VERSION,
    'PROJECT_ID' : PROJECT_ID,
    'COLLECTION' : COLLECTION,
    'SENTENCE_BUCKET' : BERT_INPUT_SENTENCE_BUCKET,
    'CLASSIFIED_SENTENCE_BUCKET' : RESOURCES_BUCKET,
    'ASSOCIATION_KEY': ASSOCIATION_KEY_LC},
    dag=dag)

monitor_classify_bl_chemical_to_gene_sentences = BashOperator(
    task_id='monitor_classify_bl_chemical_to_gene_sentences',
    bash_command="/home/airflow/gcs/data/scripts/monitor-ai-platform-job.sh classify_bl_chemical_to_gene_sentences_{{ ts_nodash }}",
    dag=dag)

# =============================================================================
#                        CLASSIFIED SENTENCE STORAGE
# =============================================================================

# # # Note: path and version are specified in the BERT relation extraction containers - so please update the containers if these paths are changed
CLASSIFIED_SENTENCE_PATH= RESOURCES_BUCKET + "/output/classified_sentences/" + BASE_IMAGE_NAME + "/" + BERT_MODEL_VERSION
CLASSIFIED_SENTENCE_FILE_NAME = BASE_IMAGE_NAME + "." + BERT_MODEL_VERSION + "." + COLLECTION + ".classified_sentences.tsv.gz"
CLASSIFIED_SENTENCE_FILE = CLASSIFIED_SENTENCE_PATH + "/" + CLASSIFIED_SENTENCE_FILE_NAME
METADATA_SENTENCE_FILE = TO_BE_CLASSIFIED_SENTENCE_BUCKET + "/" + BERT_INPUT_FILE_NAME_WITH_METADATA

classified_sentence_storage_bl_chemical_to_gene = DataflowCreateJavaJobOperator(
    task_id='classified_sentence_storage_' + ASSOCIATION_KEY_LC,
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'bertOutputFilePath': CLASSIFIED_SENTENCE_FILE,
        'sentenceMetadataFilePath': METADATA_SENTENCE_FILE,
        'biolinkAssociation': ASSOCIATION_KEY_UC,
        'databaseName': CLOUD_SQL_DATABASE_NAME,
        'dbUsername': CLOUD_SQL_DATABASE_USER,
        'dbPassword': CLOUD_SQL_DATABASE_PASSWORD,
        'mySqlInstanceName': CLOUD_SQL_INSTANCE_NAME,
        'cloudSqlRegion': CLOUD_SQL_REGION,
        'bertScoreInclusionMinimumThreshold' : BERT_SCORE_INCLUSION_THRESHOLD,
        'numWorkers': 10
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.ClassifiedSentenceStoragePipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)


# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
#                          BL:GENE_REGULATOR_RELATIONSHIP
#         EXTRACT SENTENCES, CLASSIFY, STORE RESULTS IN CLOUD SQL DB
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================

ASSOCIATION_KEY_LC = "bl_gene_regulatory_relationship"
ASSOCIATION_KEY_UC = ASSOCIATION_KEY_LC.upper()
PREFIX_X = "PR"
PLACEHOLDER_X = "@GENE_REGULATOR$"
PREFIX_Y = "PR"
PLACEHOLDER_Y = "@REGULATED_GENE$"

# name of the file containing sentences to be classified by the BERT model plus relevant sentence metadata that must accompany the classification results
BERT_INPUT_FILE_NAME_WITH_METADATA = 'bert-input-' + ASSOCIATION_KEY_LC + '.metadata.' + COLLECTION + '.tsv'

# name of the file containing all sentences that will be classified by the BERT model
BERT_INPUT_FILE_NAME = 'bert-input-' + ASSOCIATION_KEY_LC + '.' + COLLECTION + '.tsv'

# output bucket where a file containing relevant sentences to-be-classified will be stored
TO_BE_CLASSIFIED_SENTENCE_BUCKET = RESOURCES_BUCKET + '/output/sentences/' + ASSOCIATION_KEY_LC + '/' + COLLECTION

# the prefix for the files that contain the relevant to-be-classified sentences. 
# Data is processed in parallel so multiple files with this prefix will likely be created.
SENTENCE_FILE_PREFIX = TO_BE_CLASSIFIED_SENTENCE_BUCKET + '/' + ASSOCIATION_KEY_LC

# the bucket where the BERT input file will be copied after its creation
BERT_INPUT_SENTENCE_BUCKET = TO_BE_CLASSIFIED_SENTENCE_BUCKET + '/bert-input'

# clean up output sentence files - delete any sentences remaining from previous runs if any exist
delete_sentences_bl_gene_regulatory_relationship_1 = BashOperator(
    task_id='delete_old_sentences_' + ASSOCIATION_KEY_LC + '_1',
    bash_command='gsutil -q stat "' + SENTENCE_FILE_PREFIX + '*"; if [ "$?" -eq "0" ]; then gsutil rm "' + SENTENCE_FILE_PREFIX + '*"; fi',
    dag=dag)

delete_sentences_bl_gene_regulatory_relationship_2 = BashOperator(
    task_id='delete_old_sentences_' + ASSOCIATION_KEY_LC + '_2',
    bash_command='gsutil -q stat "' + SENTENCE_FILE_PREFIX + '*"; if [ "$?" -eq "0" ]; then gsutil rm "' + SENTENCE_FILE_PREFIX + '*"; fi',
    dag=dag)


## call dataflow to extract sentences with two proteins
sentence_extraction_bl_gene_regulatory_relationship = DataflowCreateJavaJobOperator(
    task_id='sentence_extraction_' + ASSOCIATION_KEY_LC,
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'targetProcessingStatusFlag': 'SENTENCE_DONE',
        'inputDocumentCriteria': "TEXT|TEXT|"+INPUT_PIPELINE_KEY+"|"+INPUT_PIPELINE_VERSION+";SECTIONS|BIONLP|"+INPUT_PIPELINE_KEY+"|"+INPUT_PIPELINE_VERSION +";SENTENCE|BIONLP|SENTENCE_SEGMENTATION|0.1.0;CONCEPT_ALL_UNFILTERED|BIONLP|CONCEPT_POST_PROCESS|0.1.0",
        'keywords':"",
        'outputBucket': SENTENCE_FILE_PREFIX,
        'prefixX': PREFIX_X,
        'placeholderX': PLACEHOLDER_X,
        'prefixY': PREFIX_Y,
        'placeholderY': PLACEHOLDER_Y,
        'collection': COLLECTION,
        'overwrite': 'YES',
        'numWorkers': 10
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.SentenceExtractionPipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)

# cat all sentences into a single file that contains the sentences + metadata, 
# then take the first 2 columns from that file and create the file that will be 
# processed by BERT then 'gsutil cp' both files to the TO_BE_CLASSIFIED_SENTENCE_BUCKET
cat_sentences_bl_gene_regulatory_relationship = BashOperator(
    task_id='cat_sentences_bl_gene_regulatory_relationship',
    bash_command="mkdir -p /home/airflow/gcs/data/to_bert && cd /home/airflow/gcs/data/to_bert && gsutil cat " + SENTENCE_FILE_PREFIX + "* > " + BERT_INPUT_FILE_NAME_WITH_METADATA + " && gsutil cp " + BERT_INPUT_FILE_NAME_WITH_METADATA + " " + TO_BE_CLASSIFIED_SENTENCE_BUCKET + " && cut -f 1-2 " + BERT_INPUT_FILE_NAME_WITH_METADATA + " > " + BERT_INPUT_FILE_NAME + " && gsutil cp " + BERT_INPUT_FILE_NAME + " " + BERT_INPUT_SENTENCE_BUCKET + "/",
    dag=dag)

# =============================================================================
#                            SENTENCE CLASSIFICATION
# =============================================================================

# call ai-platform to classify the sentences created in the previous step
# Note: There doesn't seem to be an airflow operator for calling custom containers 
#       on AI Platform at this time so we'll use a BashOperator here and will then 
#       monitor the AI Platform job until it completes.

BERT_MODEL_VERSION = "0.1"
BASE_IMAGE_NAME = ASSOCIATION_KEY_LC
PREDICT_IMAGE_NAME = BASE_IMAGE_NAME + "-predict"

# Note that the BERT prediction containers will add an output path for the classified sentences, e.g.
# [RESOURCES_BUCKET]/output/classified_sentences/bl_chemical_to_disease_or_phenotypic_feature/0.1.2

# Note: NO_ARG is a required placeholder in the gcloud ai-platform call below. If not present, the first input argument ends up missing for some reason
classify_bl_gene_regulatory_relationship_sentences = BashOperator(
    task_id='classify_' + ASSOCIATION_KEY_LC + '_sentences',
    bash_command="""
gcloud ai-platform jobs submit training "classify_${ASSOCIATION_KEY}_sentences_{{ ts_nodash }}" \
       --scale-tier basic_gpu --region "$DATAFLOW_REGION" \
       --master-image-uri "gcr.io/$PROJECT_ID/$IMAGE_NAME:$IMAGE_VERSION" \
       -- \
       "NO_ARG" \
       $SENTENCE_BUCKET \
       $COLLECTION \
       $CLASSIFIED_SENTENCE_BUCKET
""",
    env={'DATAFLOW_REGION' : DATAFLOW_REGION,
    'IMAGE_NAME' : PREDICT_IMAGE_NAME,
    'IMAGE_VERSION' : BERT_MODEL_VERSION,
    'PROJECT_ID' : PROJECT_ID,
    'COLLECTION' : COLLECTION,
    'SENTENCE_BUCKET' : BERT_INPUT_SENTENCE_BUCKET,
    'CLASSIFIED_SENTENCE_BUCKET' : RESOURCES_BUCKET,
    'ASSOCIATION_KEY': ASSOCIATION_KEY_LC},
    dag=dag)

monitor_classify_bl_gene_regulatory_relationship_sentences = BashOperator(
    task_id='monitor_classify_bl_gene_regulatory_relationship_sentences',
    bash_command="/home/airflow/gcs/data/scripts/monitor-ai-platform-job.sh classify_bl_gene_regulatory_relationship_sentences_{{ ts_nodash }}",
    dag=dag)

# =============================================================================
#                        CLASSIFIED SENTENCE STORAGE
# =============================================================================

# # # Note: path and version are specified in the BERT relation extraction containers - so please update the containers if these paths are changed
CLASSIFIED_SENTENCE_PATH= RESOURCES_BUCKET + "/output/classified_sentences/" + BASE_IMAGE_NAME + "/" + BERT_MODEL_VERSION
CLASSIFIED_SENTENCE_FILE_NAME = BASE_IMAGE_NAME + "." + BERT_MODEL_VERSION + "." + COLLECTION + ".classified_sentences.tsv.gz"
CLASSIFIED_SENTENCE_FILE = CLASSIFIED_SENTENCE_PATH + "/" + CLASSIFIED_SENTENCE_FILE_NAME
METADATA_SENTENCE_FILE = TO_BE_CLASSIFIED_SENTENCE_BUCKET + "/" + BERT_INPUT_FILE_NAME_WITH_METADATA

classified_sentence_storage_bl_gene_regulatory_relationship = DataflowCreateJavaJobOperator(
    task_id='classified_sentence_storage_' + ASSOCIATION_KEY_LC,
    jar=TM_PIPELINES_JAR,
    job_name='{{task.task_id}}',
    options={
        'bertOutputFilePath': CLASSIFIED_SENTENCE_FILE,
        'sentenceMetadataFilePath': METADATA_SENTENCE_FILE,
        'biolinkAssociation': ASSOCIATION_KEY_UC,
        'databaseName': CLOUD_SQL_DATABASE_NAME,
        'dbUsername': CLOUD_SQL_DATABASE_USER,
        'dbPassword': CLOUD_SQL_DATABASE_PASSWORD,
        'mySqlInstanceName': CLOUD_SQL_INSTANCE_NAME,
        'cloudSqlRegion': CLOUD_SQL_REGION,
        'bertScoreInclusionMinimumThreshold' : BERT_SCORE_INCLUSION_THRESHOLD,
        'numWorkers': 10
    },
    poll_sleep=10,
    job_class='edu.cuanschutz.ccp.tm_provider.etl.ClassifiedSentenceStoragePipeline',
    check_if_running=CheckJobRunning.IgnoreJob,
    location=DATAFLOW_REGION,
    retries=0,
    dag=dag
)





##### Execute full pipeline #####
dataflow_medline_xml_sentences >> prime_chebi_oger
prime_chebi_oger >> chebi_oger >> prime_cl_oger
prime_cl_oger >> cl_oger >> prime_drugbank_oger
prime_drugbank_oger >> drugbank_oger >> prime_go_bp_oger
prime_go_bp_oger >> go_bp_oger >> prime_go_cc_oger
prime_go_cc_oger >> go_cc_oger >> prime_go_mf_oger
prime_go_mf_oger >> go_mf_oger >> prime_hp_oger
prime_hp_oger >> hp_oger >> prime_mondo_oger
prime_mondo_oger >> mondo_oger >> prime_mop_oger
prime_mop_oger >> mop_oger >> prime_ncbitaxon_oger
prime_ncbitaxon_oger >> ncbitaxon_oger >> prime_pr_oger
prime_pr_oger >> pr_oger >> prime_so_oger
prime_so_oger >> so_oger >> prime_uberon_oger
prime_uberon_oger >> uberon_oger >> prime_chebi_crf
prime_chebi_crf >> chebi_crf >> prime_cl_crf
prime_cl_crf >> cl_crf >> prime_go_bp_crf
prime_go_bp_crf >> go_bp_crf >> prime_go_cc_crf
prime_go_cc_crf >> go_cc_crf >> prime_go_mf_crf
prime_go_mf_crf >> go_mf_crf >> prime_hp_crf
prime_hp_crf >> hp_crf >> prime_mondo_crf
prime_mondo_crf >> mondo_crf >> prime_mop_crf
prime_mop_crf >> mop_crf >> prime_ncbitaxon_crf
prime_ncbitaxon_crf >> ncbitaxon_crf >> prime_pr_crf
prime_pr_crf >> pr_crf >> prime_so_crf
prime_so_crf >> so_crf >> prime_uberon_crf
prime_uberon_crf >> uberon_crf
uberon_crf >> concept_post_process

concept_post_process >> concept_post_process_unfiltered

concept_post_process_unfiltered >> delete_sentences_bl_chemical_to_gene_1
concept_post_process_unfiltered >> delete_sentences_bl_gene_regulatory_relationship_1

# --- bl_chemical_to_gene
delete_sentences_bl_chemical_to_gene_1 >> sentence_extraction_bl_chemical_to_gene >> cat_sentences_bl_chemical_to_gene
cat_sentences_bl_chemical_to_gene >> classify_bl_chemical_to_gene_sentences >> monitor_classify_bl_chemical_to_gene_sentences
monitor_classify_bl_chemical_to_gene_sentences >> classified_sentence_storage_bl_chemical_to_gene >> delete_sentences_bl_chemical_to_gene_2

# --- bl_gene_regulatory_relationship
delete_sentences_bl_gene_regulatory_relationship_1 >> sentence_extraction_bl_gene_regulatory_relationship >> cat_sentences_bl_gene_regulatory_relationship
cat_sentences_bl_gene_regulatory_relationship >> classify_bl_gene_regulatory_relationship_sentences >> monitor_classify_bl_gene_regulatory_relationship_sentences
monitor_classify_bl_gene_regulatory_relationship_sentences >> classified_sentence_storage_bl_gene_regulatory_relationship >> delete_sentences_bl_gene_regulatory_relationship_2




