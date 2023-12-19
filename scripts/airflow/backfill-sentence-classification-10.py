from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import DAG
from airflow.utils import dates
from datetime import datetime, timedelta
from itertools import cycle
import time

# from airflow.operators.sensors import ExternalTaskSensor

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


# See dynamic task mapping to more efficiently start multiple tasks dynamically:
# https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html


# ====ENVIRONMENT VARIABLES THAT MUST BE SET IN CLOUD COMPOSER====
DATAFLOW_TMP_LOCATION = os.environ.get("DATAFLOW_TMP_LOCATION")
DATAFLOW_STAGING_LOCATION = os.environ.get("DATAFLOW_STAGING_LOCATION")
DATAFLOW_ZONE = os.environ.get("DATAFLOW_ZONE")
DATAFLOW_REGION = os.environ.get("DATAFLOW_REGION")

## this is the bucket that contains the ontology resources files
## this bucket also serves as a storage spot for intermediate output
RESOURCES_BUCKET = os.environ.get("RESOURCES_BUCKET")

PROJECT_ID = os.environ.get("GCP_PROJECT")

# TM_PIPELINES_JAR = the path to the tm-pipelines jar file (in some bucket)
TM_PIPELINES_JAR = os.environ.get("TM_PIPELINES_JAR")

CLOUD_SQL_DATABASE_NAME = os.environ.get("CLOUD_SQL_DATABASE_NAME")
CLOUD_SQL_DATABASE_USER = os.environ.get("CLOUD_SQL_DATABASE_USER")
CLOUD_SQL_DATABASE_PASSWORD = os.environ.get("CLOUD_SQL_DATABASE_PASSWORD")
CLOUD_SQL_INSTANCE_NAME = os.environ.get("CLOUD_SQL_INSTANCE_NAME")
CLOUD_SQL_REGION = os.environ.get("CLOUD_SQL_REGION")

INPUT_PIPELINE_KEY = "MEDLINE_XML_TO_TEXT"
INPUT_PIPELINE_VERSION = "0.1.0"

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


# COLLECTION=get_collection()

# # sentence_version = the version applied to the extracted sentence output. This version changes when changes are made to the concept recognition pipelines and a new bulk run is done.
# def get_paths(association_key_lc, sentence_version, collection, resources_bucket, bert_model_version):
#     # name of the file containing sentences to be classified by the BERT model plus relevant sentence metadata that must accompany the classification results
#     bert_input_file_name_with_metadata = 'bert-input-' + association_key_lc + '.metadata.' + sentence_version + '.' + collection + '.tsv.gz'

#     # name of the file containing all sentences that will be classified by the BERT model
#     bert_input_file_name = 'bert-input-' + association_key_lc + '.' + sentence_version + '.' + collection + '.tsv'

#     # output bucket where a file containing relevant sentences to-be-classified will be stored
#     to_be_classified_sentence_bucket = resources_bucket + '/output/sentences_tsv/' + sentence_version + '/' + association_key_lc + '/' + collection

#     # the prefix for the files that contain the relevant to-be-classified sentences.
#     # Data is processed in parallel so multiple files with this prefix will likely be created.
#     sentence_file_prefix = to_be_classified_sentence_bucket + '/' + association_key_lc

#     # the bucket where the BERT input file will be copied after its creation
#     bert_input_sentence_bucket = to_be_classified_sentence_bucket + '/bert-input'

#     # # # Note: path and version are specified in the BERT relation extraction containers - so please update the containers if these paths are changed
#     classified_sentence_path = resources_bucket + "/output/classified_sentences/" + association_key_lc + "/" + bert_model_version
#     classified_sentence_file_name  = association_key_lc + "." + bert_model_version + "." + collection + ".classified_sentences.tsv.gz"
#     classified_sentence_file = classified_sentence_path + "/" + classified_sentence_file_name
#     metadata_sentence_file = to_be_classified_sentence_bucket + "/" + bert_input_file_name_with_metadata

#     ancestor_map_file_path = resources_bucket + "/ontology-resources/ontology-class-ancestor-map.tsv.gz"

#     return bert_input_file_name_with_metadata, bert_input_file_name, to_be_classified_sentence_bucket, sentence_file_prefix, bert_input_sentence_bucket, classified_sentence_file, metadata_sentence_file, ancestor_map_file_path

# sentence_version = the version applied to the extracted sentence output. This version changes when changes are made to the concept recognition pipelines and a new bulk run is done.


def get_paths(
    association_key_lc,
    association_key_lc_no_association,
    sentence_version,
    collection_prefix,
    collection_index,
    resources_bucket,
    bert_model_version,
    dataflow_region,
    project_id
):
    collection = collection_prefix + str(collection_index)

    # name of the file containing sentences to be classified by the BERT model plus relevant sentence metadata that must accompany the classification results
    bert_input_file_name_with_metadata = (
        "bert-input-"
        + association_key_lc
        + ".metadata."
        + sentence_version
        + "."
        + collection
        + ".tsv.gz"
    )

    # name of the file containing all sentences that will be classified by the BERT model
    bert_input_file_name = (
        "bert-input-"
        + association_key_lc
        + "."
        + sentence_version
        + "."
        + collection
        + ".tsv"
    )

    # output bucket where a file containing relevant sentences to-be-classified will be stored
    to_be_classified_sentence_bucket = (
        resources_bucket
        + "/output/sentences_tsv/"
        + sentence_version
        + "/"
        + association_key_lc
        + "/"
        + collection
    )

    # the prefix for the files that contain the relevant to-be-classified sentences.
    # Data is processed in parallel so multiple files with this prefix will likely be created.
    sentence_file_prefix = to_be_classified_sentence_bucket + "/" + association_key_lc

    # the bucket where the BERT input file will be copied after its creation
    bert_input_sentence_bucket = to_be_classified_sentence_bucket + "/bert-input"

    # # # Note: path and version are specified in the BERT relation extraction containers - so please update the containers if these paths are changed
    classified_sentence_path = (
        resources_bucket
        + "/output/classified_sentences/"
        + "sent_" + sentence_version
        + "/"
        + association_key_lc
        + "/model_"
        + bert_model_version
    )
    classified_sentence_file_name = (
        association_key_lc
        + "." + sentence_version
        + "_"
        + bert_model_version
        + "."
        + collection
        + ".classified_sentences.tsv.gz"
    )
    classified_sentence_file = (
        classified_sentence_path + "/" + classified_sentence_file_name
    )
    metadata_sentence_file = (
        to_be_classified_sentence_bucket + "/" + bert_input_file_name_with_metadata
    )

    val = {
        "collection": collection,
        "bert_input_file_name_with_metadata": bert_input_file_name_with_metadata,
        "bert_input_file_name": bert_input_file_name,
        "to_be_classified_sentence_bucket": to_be_classified_sentence_bucket,
        "sentence_file_prefix": sentence_file_prefix,
        "bert_input_sentence_bucket": bert_input_sentence_bucket,
        "classified_sentence_file": classified_sentence_file,
        "metadata_sentence_file": metadata_sentence_file,
        "dataflow_region" : dataflow_region,
        "image_name": association_key_lc_no_association + "-predict",
        "image_version": bert_model_version,
        "project_id": project_id,
        "association_key_lc" : association_key_lc,
        "resources_bucket" : resources_bucket,
        "timestamp" : "T" + str(time.time()).replace(".", "_"),
        "sentence_version" : sentence_version
    }


    print(f'path group: {val}')
    return val


def get_delete_sentences_operator(
    dag, collection, association_key_lc, sentence_file_prefix
):
    return BashOperator(
        task_id="delete_old_sentences_" + association_key_lc + "_" + collection.lower(),
        bash_command='gsutil -q stat "'
        + sentence_file_prefix
        + '*"; if [ "$?" -eq "0" ]; then gsutil rm "'
        + sentence_file_prefix
        + '*"; fi',
        dag=dag,
    )


# def get_sentence_extraction_pipeline_operator(dag, ASSOCIATION_KEY_LC, ASSOCIATION_KEY_LC_NO_ASSOCIATION, tm_pipelines_jar, input_pipeline_key, input_pipeline_version, sentence_file_prefix, prefix_x, placeholder_x, prefix_y, placeholder_y, collection, dataflow_region, ancestor_map_file_path):
#     return DataflowCreateJavaJobOperator(
#         task_id='sentence_extraction_' + association_key_lc + '_' + collection.lower(),
#         jar=tm_pipelines_jar,
#         job_name='{{task.task_id}}',
#         options={
#             'targetProcessingStatusFlag': 'SENTENCE_DONE',
#             'inputDocumentCriteria': "TEXT|TEXT|"+input_pipeline_key+"|"+input_pipeline_version+";SECTIONS|BIONLP|"+input_pipeline_key+"|"+input_pipeline_version +";SENTENCE|BIONLP|SENTENCE_SEGMENTATION|0.1.0;CONCEPT_ALL|BIONLP|CONCEPT_POST_PROCESS|0.1.0",
#             'keywords':"",
#             'conceptIdsToExclude':'CHEBI:36080|PR:000003944|PR:000011336|CL:0000000|PR:000000001',
#             'outputBucket': sentence_file_prefix,
#             'prefixX': prefix_x,
#             'placeholderX': placeholder_x,
#             'prefixY': prefix_y,
#             'placeholderY': placeholder_y,
#             'collection': collection,
#             'overwrite': 'YES',
#             'ancestorMapFilePath': ancestor_map_file_path,
#             'ancestorMapFileDelimiter': 'TAB',
#             'ancestorMapFileSetDelimiter': 'PIPE',
#             'numWorkers': 25,
#             'maxNumWorkers': 50,
#             'autoscalingAlgorithm': "THROUGHPUT_BASED"
#         },
#         poll_sleep=10,
#         job_class='edu.cuanschutz.ccp.tm_provider.etl.SentenceExtractionPipeline',
#         check_if_running=CheckJobRunning.IgnoreJob,
#         location=dataflow_region,
#         retries=0,
#         dag=dag)


# class BulkCopySentencesToClassifyOperator(BashOperator):
#     template_fields = ('bash_command', 'association_key_lc', 'resources_bucket', 'sentence_version', 'collection_prefix', 'collection_index')

#     @apply_defaults
#     def __init__(
#             self,
#             association_key_lc,
#             resources_bucket,
#             sentence_version,
#             collection_prefix,
#             collection_index,
#             *args, **kwargs):
#         # name of the file containing sentences to be classified by the BERT model plus relevant sentence metadata that must accompany the classification results
#         bert_input_file_name_with_metadata = 'bert-input-' + association_key_lc + '.metadata.' + sentence_version + '.' + collection_prefix + str(collection_index) + '.tsv'

#         # output bucket where a file containing relevant sentences to-be-classified will be stored
#         # example: translator-text-workflow-dev_work/output/sentences_tsv/0.2.0/bl_chemical_to_disease_or_phenotypic_feature/PUBMED_SUB_0
#         to_be_classified_sentence_bucket = resources_bucket + '/output/sentences_tsv/' + sentence_version + "/" + association_key_lc + '/' + collection_prefix + str(collection_index)

#         # the prefix for the files that contain the relevant to-be-classified sentences.
#         # Data is processed in parallel so multiple files with this prefix will likely be created.
#         sentence_file_prefix = to_be_classified_sentence_bucket + '/' + association_key_lc

#         # bucket that will aggregate all data from all collections for this version of sentences
#         aggregated_sentence_bucket = resources_bucket + '/output/sentences_tsv/' + sentence_version + "/" + association_key_lc + "/aggregated"

#         # name of the file containing all sentences that will be classified by the BERT model
#         bert_input_file_name = 'bert-input-' + association_key_lc + '.' + sentence_version + '.' + collection_prefix + str(collection_index) + '.tsv'

#         # the bucket where the BERT input file will be copied after its creation
#         bert_input_sentence_bucket = aggregated_sentence_bucket + '/bert-input'
#         super(BulkCopySentencesToClassifyOperator, self).__init__(bash_command="mkdir -p /home/airflow/gcs/data/to_bert && cd /home/airflow/gcs/data/to_bert && gsutil cat " + sentence_file_prefix + "* > " + bert_input_file_name_with_metadata + " && gsutil cp " + bert_input_file_name_with_metadata + " " + aggregated_sentence_bucket + " && cut -f 1-2 " + bert_input_file_name_with_metadata + " > " + bert_input_file_name + " && gsutil cp " + bert_input_file_name + " " + bert_input_sentence_bucket + "/", *args, **kwargs)
#         self.association_key_lc = association_key_lc
#         self.resources_bucket = resources_bucket
#         self.sentence_version = sentence_version
#         self.collection_prefix = collection_prefix
#         self.collection_index = collection_index

# def get_bulk_copy_sentences_to_classify_operator(dag, association_key_lc, resources_bucket, sentence_version, collection_prefix, collection_index):
#     # name of the file containing sentences to be classified by the BERT model plus relevant sentence metadata that must accompany the classification results
#     bert_input_file_name_with_metadata = 'bert-input-' + association_key_lc + '.metadata.' + sentence_version + '.' + collection_prefix + str(collection_index) + '.tsv'

#     # output bucket where a file containing relevant sentences to-be-classified will be stored
#     # example: translator-text-workflow-dev_work/output/sentences_tsv/0.2.0/bl_chemical_to_disease_or_phenotypic_feature/PUBMED_SUB_0
#     to_be_classified_sentence_bucket = resources_bucket + '/output/sentences_tsv/' + sentence_version + "/" + association_key_lc + '/' + collection_prefix + str(collection_index)

#     # the prefix for the files that contain the relevant to-be-classified sentences.
#     # Data is processed in parallel so multiple files with this prefix will likely be created.
#     sentence_file_prefix = to_be_classified_sentence_bucket + '/' + association_key_lc

#     # bucket that will aggregate all data from all collections for this version of sentences
#     aggregated_sentence_bucket = resources_bucket + '/output/sentences_tsv/' + sentence_version + "/" + association_key_lc + "/aggregated"

#     # name of the file containing all sentences that will be classified by the BERT model
#     bert_input_file_name = 'bert-input-' + association_key_lc + '.' + sentence_version + '.' + collection_prefix + str(collection_index) + '.tsv'

#      # the bucket where the BERT input file will be copied after its creation
#     bert_input_sentence_bucket = aggregated_sentence_bucket + '/bert-input'

#     return BashOperator(
#         task_id='bulk_cat_sentences_' + association_key_lc + "_" + sentence_version.replace(".","_"),
#         bash_command="mkdir -p /home/airflow/gcs/data/to_bert && cd /home/airflow/gcs/data/to_bert && gsutil cat " + sentence_file_prefix + "* > " + bert_input_file_name_with_metadata + " && gsutil cp " + bert_input_file_name_with_metadata + " " + aggregated_sentence_bucket + " && cut -f 1-2 " + bert_input_file_name_with_metadata + " > " + bert_input_file_name + " && gsutil cp " + bert_input_file_name + " " + bert_input_sentence_bucket + "/",
#         #trigger_rule="all_done",
#         dag=dag)


# sentence_version = the version applied to the extracted sentence output. This version changes when changes are made to the concept recognition pipelines and a new bulk run is done.
# def get_bulk_cat_sentences_operator(dag, sentence_version):
#     return BashOperator(
#         task_id='bulk_cat_sentences_' + association_key_lc + "_" + sentence_version.replace(".","_"),
#         bash_command="mkdir -p /home/airflow/gcs/data/to_bert && cd /home/airflow/gcs/data/to_bert && gsutil cat " + sentence_file_prefix + "* > " + bert_input_file_name_with_metadata + " && gsutil cp " + bert_input_file_name_with_metadata + " " + to_be_classified_sentence_bucket + " && cut -f 1-2 " + bert_input_file_name_with_metadata + " > " + bert_input_file_name + " && gsutil cp " + bert_input_file_name + " " + bert_input_sentence_bucket + "/",
#         trigger_rule="all_done",
#         dag=dag)


# def get_cat_sentences_operator(
#     dag,
#     collection,
#     association_key_lc,
#     sentence_file_prefix,
#     bert_input_file_name_with_metadata,
#     to_be_classified_sentence_bucket,
#     bert_input_file_name,
#     bert_input_sentence_bucket,
# ):
#     return BashOperator(
#         task_id="cat_sentences_" + association_key_lc + "_" + collection.lower(),
#         bash_command="mkdir -p /home/airflow/gcs/data/to_bert && cd /home/airflow/gcs/data/to_bert && gsutil cat "
#         + sentence_file_prefix
#         + "* > "
#         + bert_input_file_name_with_metadata
#         + " && gsutil cp "
#         + bert_input_file_name_with_metadata
#         + " "
#         + to_be_classified_sentence_bucket
#         + " && gunzip -c "
#         + bert_input_file_name_with_metadata
#         + " | cut -f 1-2 > "
#         + bert_input_file_name
#         + " && gsutil cp "
#         + bert_input_file_name
#         + " "
#         + bert_input_sentence_bucket
#         + "/",
#         trigger_rule="all_done",
#         dag=dag,
#     )


def get_sentence_classifier_operator(
    dag,
    association_key_lc,
    association_key_lc_no_association,
    dataflow_region,
    bert_model_version,
    project_id,
    collection,
    bert_input_sentence_bucket,
    resources_bucket,
):
    return BashOperator(
        task_id="classify_" + association_key_lc + "_sentences_" + collection.lower(),
        bash_command="""
    gcloud ai-platform jobs submit training "classify_${ASSOCIATION_KEY}_sentences_${COLLECTION}_{{ ts_nodash }}" \
        --scale-tier basic_gpu --region "$DATAFLOW_REGION" \
        --master-image-uri "gcr.io/$PROJECT_ID/$IMAGE_NAME:$IMAGE_VERSION" \
        -- \
        "NO_ARG" \
        $SENTENCE_BUCKET \
        $COLLECTION \
        $CLASSIFIED_SENTENCE_BUCKET
    """,
        env={
            "DATAFLOW_REGION": dataflow_region,
            "IMAGE_NAME": association_key_lc_no_association + "-predict",
            "IMAGE_VERSION": bert_model_version,
            "PROJECT_ID": project_id,
            "COLLECTION": collection,
            "SENTENCE_BUCKET": bert_input_sentence_bucket,
            "CLASSIFIED_SENTENCE_BUCKET": resources_bucket,
            "ASSOCIATION_KEY": association_key_lc,
        },
        dag=dag,
    )


def get_aiplatform_monitor_operator(dag, collection, association_key_lc):
    return BashOperator(
        task_id="monitor_classify_"
        + association_key_lc
        + "_sentences_"
        + collection.lower(),
        bash_command="/home/airflow/gcs/data/scripts/monitor-ai-platform-job.sh classify_${ASSOCIATION_KEY}_sentences_${COLLECTION}_{{ ts_nodash }}",
        env={"ASSOCIATION_KEY": association_key_lc, "COLLECTION": collection},
        dag=dag,
    )


def get_sentence_storage_pipeline(
    dag,
    collection,
    association_key_lc,
    tm_pipelines_jar,
    classified_sentence_file,
    metadata_sentence_file,
    cloud_sql_database_name,
    cloud_sql_database_user,
    cloud_sql_database_password,
    cloud_sql_instance_name,
    cloud_sql_region,
    bert_score_inclusion_threshold,
    database_version,
    dataflow_region,
):
    return DataflowCreateJavaJobOperator(
        task_id="classified_sentence_storage_"
        + association_key_lc
        + "_"
        + collection.lower(),
        jar=tm_pipelines_jar,
        job_name="{{task.task_id}}",
        options={
            "bertOutputFilePath": classified_sentence_file,
            "sentenceMetadataFilePath": metadata_sentence_file,
            "biolinkAssociation": association_key_lc.upper(),
            "databaseName": cloud_sql_database_name,
            "dbUsername": cloud_sql_database_user,
            "dbPassword": cloud_sql_database_password,
            "mySqlInstanceName": cloud_sql_instance_name,
            "cloudSqlRegion": cloud_sql_region,
            "bertScoreInclusionMinimumThreshold": bert_score_inclusion_threshold,
            "databaseVersion": database_version,
            "numWorkers": 25,
            "maxNumWorkers": 50,
            "autoscalingAlgorithm": "THROUGHPUT_BASED",
        },
        poll_sleep=10,
        job_class="edu.cuanschutz.ccp.tm_provider.etl.ClassifiedSentenceStoragePipeline",
        check_if_running=CheckJobRunning.IgnoreJob,
        location=dataflow_region,
        retries=0,
        dag=dag,
    )


# ==================DAG ARGUMENTS==============================

args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 8, 24),
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
    "dataflow_default_options": {
        "zone": DATAFLOW_ZONE,
        "region": DATAFLOW_REGION,
        "stagingLocation": DATAFLOW_STAGING_LOCATION,
        "gcpTempLocation": DATAFLOW_TMP_LOCATION,
    },
}

#dag = DAG(dag_id="backfill-assertions", default_args=args, catchup=False)

# COLLECTION_PREFIX = "PUBMED_SUB_"
COLLECTION_PREFIX='PMC_SUBSET_'
SENTENCE_VERSION = "0.2.0"

# # chem-dis
# BERT_MODEL_VERSION = "0.4"
# ASSOCIATION_KEY_LC = "bl_chemical_to_disease_or_phenotypic_feature"
# ASSOCIATION_KEY_LC_NO_ASSOCIATION = "bl_chemical_to_disease_or_phenotypic_feature"

# # chem-gene
# BERT_MODEL_VERSION = "0.1"
# ASSOCIATION_KEY_LC = "bl_chemical_to_gene"
# ASSOCIATION_KEY_LC_NO_ASSOCIATION = "bl_chemical_to_gene"

# gene-gene
BERT_MODEL_VERSION = "0.1"
ASSOCIATION_KEY_LC = "bl_gene_regulatory_relationship"
ASSOCIATION_KEY_LC_NO_ASSOCIATION = "bl_gene_regulatory_relationship"

# (
#     BERT_INPUT_FILE_NAME_WITH_METADATA,
#     BERT_INPUT_FILE_NAME,
#     TO_BE_CLASSIFIED_SENTENCE_BUCKET,
#     SENTENCE_FILE_PREFIX,
#     BERT_INPUT_SENTENCE_BUCKET,
#     CLASSIFIED_SENTENCE_FILE,
#     METADATA_SENTENCE_FILE,
#     ANCESTOR_MAP_FILE_PATH,
# ) = get_paths(
#     ASSOCIATION_KEY_LC,
#     SENTENCE_VERSION,
#     COLLECTION,
#     RESOURCES_BUCKET,
#     BERT_MODEL_VERSION,
# )
# cat_sentences_bl_chemical_to_disease_bulk = get_cat_sentences_operator(dag, COLLECTION, ASSOCIATION_KEY_LC, SENTENCE_FILE_PREFIX, BERT_INPUT_FILE_NAME_WITH_METADATA, TO_BE_CLASSIFIED_SENTENCE_BUCKET, BERT_INPUT_FILE_NAME, BERT_INPUT_SENTENCE_BUCKET)
# classify_bl_chemical_to_disease_sentences_bulk = get_sentence_classifier_operator(dag, ASSOCIATION_KEY_LC, ASSOCIATION_KEY_LC_NO_ASSOCIATION, DATAFLOW_REGION, BERT_MODEL_VERSION, PROJECT_ID, COLLECTION, BERT_INPUT_SENTENCE_BUCKET, RESOURCES_BUCKET)

with DAG(
    "backfill-assertions-10",
    start_date=datetime(2021, 8, 24),
    catchup=False,
    schedule_interval=None
    # owner="airflow",
    # depends_on_past=False,
    # retries=0,
    # retry_delay=timedelta(minutes=2)
    ):
    
    # this results in a list of all argument groups, 1 per collection
    zipped_arguments = list(
        zip(
            cycle([ASSOCIATION_KEY_LC]),
            cycle([ASSOCIATION_KEY_LC_NO_ASSOCIATION]),
            cycle([SENTENCE_VERSION]),
            cycle([COLLECTION_PREFIX]),
            range(20,22),
            cycle([RESOURCES_BUCKET]),
            cycle([BERT_MODEL_VERSION]),
            cycle([DATAFLOW_REGION]),
            cycle([PROJECT_ID]),
        )
    )

    # call get_paths over all argument groups, results in 1 path group per collection
    get_all_path_groups = PythonOperator.partial(
        task_id="get_all_path_groups",
        python_callable=get_paths,
    ).expand(op_args=zipped_arguments)

    # for each path group (1 per collection) prepare the extracted sentences 
    # for BERT consumption by cat'ing the individual files (maintaining 
    # distinct collections) and uploading them to the proper bucket
    cat_sentence_op = BashOperator.partial(
            task_id="cat_sentences_" + ASSOCIATION_KEY_LC,
            append_env=True,
            execution_timeout=timedelta(seconds=1200),
            bash_command="mkdir -p /home/airflow/gcs/data/to_bert && cd /home/airflow/gcs/data/to_bert && gsutil -m cat $sentence_file_prefix* > $bert_input_file_name_with_metadata && gsutil -m cp $bert_input_file_name_with_metadata $to_be_classified_sentence_bucket && gunzip -c $bert_input_file_name_with_metadata | cut -f 1-2 > $bert_input_file_name && gsutil -m cp $bert_input_file_name $bert_input_sentence_bucket/",
        ).expand(env=get_all_path_groups.output)

    # for each collection, classify the sentences that were extracted
    #
    # Note: There was a missing permission for the composer service account to 
    # start AI-platform jobs. This permissions issue may have confounded an 
    # issue I was also having with environment variables not being transfered 
    # to the bash_command. So the $(echo "gcloud ...") might not be necessary 
    # -- it may have just worked with "gcloud ..." but I am not sure at this point.  
    # classify_sentence_op = BashOperator.partial(
    #         task_id="classify_sentences_" + ASSOCIATION_KEY_LC,
    #         append_env=True,
    #         execution_timeout=timedelta(seconds=1200),
    #         # bash_command='gcloud ai-platform jobs submit training "classify_${association_key_lc}_sentences_${collection}_${timestamp}" --scale-tier basic_gpu --region "${dataflow_region}" --master-image-uri "gcr.io/${project_id}/${image_name}:${image_version}" -- NO_ARG "${bert_input_sentence_bucket}" "${collection}" "${resources_bucket}"',
    #         # bash_command="echo \"TIMESTAMP: $timestamp gcloud ai-platform jobs submit training classify_${association_key_lc}_sentences_${collection}_${timestamp} --scale-tier basic_gpu --region ${dataflow_region} --master-image-uri gcr.io/${project_id}/${image_name}:${image_version} -- NO_ARG ${bert_input_sentence_bucket} ${collection} ${resources_bucket}\"",
    #         bash_command="$(echo \"gcloud ai-platform jobs submit training classify_${association_key_lc}_sentences_${collection}_${timestamp} --scale-tier basic_gpu --region ${dataflow_region} --master-image-uri gcr.io/${project_id}/${image_name}:${image_version} -- NO_ARG ${bert_input_sentence_bucket} ${collection} ${resources_bucket} ${sentence_version}\")",
    #     ).expand(env=get_all_path_groups.output)


# for some reason, had to run these as two separate steps in order to get it to work
    cat_sentence_op #>> 
    # classify_sentence_op
