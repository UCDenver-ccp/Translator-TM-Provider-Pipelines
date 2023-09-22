#!/bin/bash

# Running this script results in the export of the actionable text documents to a google bucket.
# This step is used to export the text so that it can be processed outside of Dataflow, by AI Platform for example.

source ./rrun.env.sh

########### CHOOSE STAGE OR PROD
## STAGE
# PROJECT_ID=lithe-vault-265816
# WORK_BUCKET=gs://translator-tm-provider-datastore-staging-stage

# ## PROD
# PROJECT_ID=translator-text-workflow-dev
# WORK_BUCKET=gs://translator-text-workflow-dev_work

########### CHOOSE MEDLINE OR PMCOA
#### MEDLINE
TEXT_PIPELINE_KEY=MEDLINE_XML_TO_TEXT
TEXT_PIPELINE_VERSION='0.1.0'
SUBSET_PREFIX=PUBMED_SUB_
MAX_SUBSET_INDEX=37

#### PMCOA
# PIPELINE_KEY=BIOC_TO_TEXT
# TEXT_PIPELINE_VERSION=
# SUBSET_PREFIX=PMC_SUBSET_
# MAX_SUBSET_INDEX=36

SCRIPT=./scripts/pipelines/export/run_document_text_export.sh

# STAGE_LOCATION=$WORK_BUCKET/staging 
# TEMP_LOCATION=$WORK_BUCKET/temp
OVERWRITE_FLAG=YES

JAR_VERSION='0.2.1'

echo "Starting document text export"
COLLECTION=PUBMED_SUB_30
#COLLECTION=TEST
OUTPUT_BUCKET="$WORK_BUCKET/output/text-export/$COLLECTION/text-export"
$SCRIPT $PROJECT_ID $COLLECTION $TEXT_PIPELINE_KEY $TEXT_PIPELINE_VERSION $OVERWRITE_FLAG $OUTPUT_BUCKET ${STAGE_LOCATION} ${TEMP_LOCATION} ${JAR_VERSION} &> "./logs/text-export-${COLLECTION}.log"

# for INDEX in $(seq 0 1 $MAX_SUBSET_INDEX)  
#   do 
#     ind=$(($INDEX + 0))
#     if (( ind <= $MAX_SUBSET_INDEX)); then
#         echo "Starting abbreviation detection pipeline... ${ind} $(date)"
#         COLLECTION=${SUBSET_PREFIX}${ind}
#         OUTPUT_BUCKET="$WORK_BUCKET/output/text-export/$COLLECTION/text-export"
#         $SCRIPT $PROJECT_ID $COLLECTION $PIPELINE_KEY $OVERWRITE_FLAG $OUTPUT_BUCKET ${STAGE_LOCATION} ${TEMP_LOCATION} ${JAR_VERSION} &> ./logs/text-export-${ind}.log &
#         sleep 120
#     fi
#     ind=$(($INDEX + 1))
#     if (( ind <= $MAX_SUBSET_INDEX)); then
#         echo "Starting abbreviation detection pipeline... ${ind} $(date)"
#         COLLECTION=${SUBSET_PREFIX}${ind}
#         OUTPUT_BUCKET="$WORK_BUCKET/output/text-export/$COLLECTION/text-export"
#         $SCRIPT $PROJECT_ID $COLLECTION $PIPELINE_KEY $OVERWRITE_FLAG $OUTPUT_BUCKET ${STAGE_LOCATION} ${TEMP_LOCATION} ${JAR_VERSION} &> ./logs/text-export-${ind}.log &
#         sleep 120
#     fi
#     ind=$(($INDEX + 2))
#     if (( ind <= $MAX_SUBSET_INDEX)); then
#         echo "Starting abbreviation detection pipeline... ${ind} $(date)"
#         COLLECTION=${SUBSET_PREFIX}${ind}
#         OUTPUT_BUCKET="$WORK_BUCKET/output/text-export/$COLLECTION/text-export"
#         $SCRIPT $PROJECT_ID $COLLECTION $PIPELINE_KEY $OVERWRITE_FLAG $OUTPUT_BUCKET ${STAGE_LOCATION} ${TEMP_LOCATION} ${JAR_VERSION} &> ./logs/text-export-${ind}.log &
#         sleep 120
#     fi
#     ind=$(($INDEX + 3))
#     if (( ind <= $MAX_SUBSET_INDEX)); then
#         echo "Starting abbreviation detection pipeline... ${ind} $(date)"
#         COLLECTION=${SUBSET_PREFIX}${ind}
#         OUTPUT_BUCKET="$WORK_BUCKET/output/text-export/$COLLECTION/text-export"
#         $SCRIPT $PROJECT_ID $COLLECTION $PIPELINE_KEY $OVERWRITE_FLAG $OUTPUT_BUCKET ${STAGE_LOCATION} ${TEMP_LOCATION} ${JAR_VERSION} &> ./logs/text-export-${ind}.log &
#     fi
#     wait 
#   done