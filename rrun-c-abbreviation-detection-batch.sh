#!/bin/bash

source ./rrun.env.sh

SOURCE_PATH="${WORK_BUCKET}/code-dependencies/ab3p-abbreviation/"

########## CHOOSE MEDLINE OR PMCOA
#### MEDLINE
TEXT_PIPELINE_KEY=MEDLINE_XML_TO_TEXT
TEXT_PIPELINE_VERSION="recent"
SUBSET_PREFIX=PUBMED_SUB_
MAX_SUBSET_INDEX=36

# #### PMCOA
# TEXT_PIPELINE_KEY=BIOC_TO_TEXT
# TEXT_PIPELINE_VERSION="0.1.0"
# SUBSET_PREFIX=PMC_SUBSET_
# MAX_SUBSET_INDEX=37

SCRIPT=./scripts/pipelines/concepts/run_abbreviations.sh
WORKER_PATH=/tmp/abbrev

OVERWRITE=YES
SENTENCE_PIPELINE_KEY="SENTENCE_SEGMENTATION"
SENTENCE_PIPELINE_VERSION="recent"

OUTPUT_PIPELINE_VERSION="0.3.0"

# # use the below to run a single collection
# echo "Starting abbreviation detection pipeline..."
# COLLECTION=PUBMED_SUB_37
# $SCRIPT $SOURCE_PATH $WORKER_PATH $TEXT_PIPELINE_KEY $TEXT_PIPELINE_VERSION $SENTENCE_PIPELINE_KEY $SENTENCE_PIPELINE_VERSION $OUTPUT_PIPELINE_VERSION $PROJECT_ID ${COLLECTION} ${STAGE_LOCATION} ${TEMP_LOCATION} ${JAR_VERSION} ${OVERWRITE} &> "./logs/abbreviation-detect-${COLLECTION}.log" &


for INDEX in $(seq 0 4 $MAX_SUBSET_INDEX)  
  do 
    ind=$(($INDEX + 0))
    if (( ind <= $MAX_SUBSET_INDEX)); then
        echo "Starting abbreviation detection pipeline... ${ind} $(date)"
        COLLECTION="${SUBSET_PREFIX}${ind}"
        $SCRIPT $SOURCE_PATH $WORKER_PATH $TEXT_PIPELINE_KEY $TEXT_PIPELINE_VERSION $SENTENCE_PIPELINE_KEY $SENTENCE_PIPELINE_VERSION $OUTPUT_PIPELINE_VERSION $PROJECT_ID ${COLLECTION} ${STAGE_LOCATION} ${TEMP_LOCATION} ${JAR_VERSION} ${OVERWRITE} &> ./logs/abbreviation-detect-${COLLECTION}.log &
        sleep 120
    fi
    ind=$(($INDEX + 1))
    if (( ind <= $MAX_SUBSET_INDEX)); then
        echo "Starting abbreviation detection pipeline... ${ind} $(date)"
        COLLECTION="${SUBSET_PREFIX}${ind}"
        $SCRIPT $SOURCE_PATH $WORKER_PATH $TEXT_PIPELINE_KEY $TEXT_PIPELINE_VERSION $SENTENCE_PIPELINE_KEY $SENTENCE_PIPELINE_VERSION $OUTPUT_PIPELINE_VERSION $PROJECT_ID ${COLLECTION} ${STAGE_LOCATION} ${TEMP_LOCATION} ${JAR_VERSION} ${OVERWRITE} &> ./logs/abbreviation-detect-${COLLECTION}.log &
        sleep 120
    fi
    ind=$(($INDEX + 2))
    if (( ind <= $MAX_SUBSET_INDEX)); then
        echo "Starting abbreviation detection pipeline... ${ind} $(date)"
        COLLECTION="${SUBSET_PREFIX}${ind}"
        $SCRIPT $SOURCE_PATH $WORKER_PATH $TEXT_PIPELINE_KEY $TEXT_PIPELINE_VERSION $SENTENCE_PIPELINE_KEY $SENTENCE_PIPELINE_VERSION $OUTPUT_PIPELINE_VERSION $PROJECT_ID ${COLLECTION} ${STAGE_LOCATION} ${TEMP_LOCATION} ${JAR_VERSION} ${OVERWRITE} &> ./logs/abbreviation-detect-${COLLECTION}.log &
        sleep 120
    fi
    ind=$(($INDEX + 3))
    if (( ind <= $MAX_SUBSET_INDEX)); then
        echo "Starting abbreviation detection pipeline... ${ind} $(date)"
        COLLECTION="${SUBSET_PREFIX}${ind}"
        $SCRIPT $SOURCE_PATH $WORKER_PATH $TEXT_PIPELINE_KEY $TEXT_PIPELINE_VERSION $SENTENCE_PIPELINE_KEY $SENTENCE_PIPELINE_VERSION $OUTPUT_PIPELINE_VERSION $PROJECT_ID ${COLLECTION} ${STAGE_LOCATION} ${TEMP_LOCATION} ${JAR_VERSION} ${OVERWRITE} &> ./logs/abbreviation-detect-${COLLECTION}.log &
    fi
    wait 
  done