#!/bin/bash

source ./rrun.env.sh

# This script imports dependency parses that reside in a google bucket in CoNLL-U format into Datastore.
# The dependency parses are consolidated into one or more files in the google bucket. This pipeline separates the parses for
# individual documents and stores them as individual documents in Datastore.


SCRIPT=./scripts/pipelines/import/run_dependency_parse_import.sh
OVERWRITE_FLAG=YES
SENTENCE_PIPELINE_VERSION="0.2.0"

BASE_DP_FILE_PATH="${WORK_BUCKET}/output/dependency_parses"
JAR_VERSION="0.2.1"


# COLLECTION=PUBMED_SUB_30
COLLECTION=TEST

$SCRIPT $PROJECT_ID $COLLECTION $BASE_DP_FILE_PATH $OVERWRITE_FLAG ${STAGE_LOCATION} ${TEMP_LOCATION} ${JAR_VERSION} &> "./logs/dp-import-${COLLECTION}.log"

# for INDEX in $(seq 0 4 $MAX_SUBSET_INDEX)  
#   do 
#     ind=$(($INDEX + 0))
#     if (( ind <= $MAX_SUBSET_INDEX)); then
#         echo "Starting abbreviation detection pipeline... ${ind} $(date)"
#         $SCRIPT $SOURCE_PATH $WORKER_PATH $PIPELINE_KEY $PROJECT_ID ${SUBSET_PREFIX}${ind} ${STAGE_LOCATION} ${TEMP_LOCATION} &> ./logs/abbreviation-detect-${ind}.log &
#         sleep 120
#     fi
#     ind=$(($INDEX + 1))
#     if (( ind <= $MAX_SUBSET_INDEX)); then
#         echo "Starting abbreviation detection pipeline... ${ind} $(date)"
#         $SCRIPT $SOURCE_PATH $WORKER_PATH $PIPELINE_KEY $PROJECT_ID ${SUBSET_PREFIX}${ind} ${STAGE_LOCATION} ${TEMP_LOCATION} &> ./logs/abbreviation-detect-${ind}.log &
#         sleep 120
#     fi
#     ind=$(($INDEX + 2))
#     if (( ind <= $MAX_SUBSET_INDEX)); then
#         echo "Starting abbreviation detection pipeline... ${ind} $(date)"
#         $SCRIPT $SOURCE_PATH $WORKER_PATH $PIPELINE_KEY $PROJECT_ID ${SUBSET_PREFIX}${ind} ${STAGE_LOCATION} ${TEMP_LOCATION} &> ./logs/abbreviation-detect-${ind}.log &
#         sleep 120
#     fi
#     ind=$(($INDEX + 3))
#     if (( ind <= $MAX_SUBSET_INDEX)); then
#         echo "Starting abbreviation detection pipeline... ${ind} $(date)"
#         $SCRIPT $SOURCE_PATH $WORKER_PATH $PIPELINE_KEY $PROJECT_ID ${SUBSET_PREFIX}${ind} ${STAGE_LOCATION} ${TEMP_LOCATION} &> ./logs/abbreviation-detect-${ind}.log &
#     fi
#     wait 
#   done