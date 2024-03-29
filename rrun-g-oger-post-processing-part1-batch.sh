#!/bin/sh

# This script runs the first part of concept post-processing. The large side-
# input of the concept id to dictionary entries map was causing the original 
# concept post processing pipeline to stall, so we have split some of the post
# processing into smaller chunks to avoid such pipeline stalls. This first step
# in the concept post-processing takes as input the CS, CIMIN, anc CIMAX OGER output
# and removes spurious annotations using roughly half of the id-to-dict-entry file. 
# The other half of the file is used in the following script.

source ./rrun.env.sh

SCRIPT=./scripts/pipelines/concepts/run_oger_post_process.sh

# TEXT_PIPELINE_KEY=MEDLINE_XML_TO_TEXT
# TEXT_PIPELINE_VERSION="0.1.1"
# MAX_SUBSET_INDEX=36
# SUBSET_PREFIX=PUBMED_SUB_

TEXT_PIPELINE_KEY=FILTER_UNACTIONABLE_TEXT
TEXT_PIPELINE_VERSION="recent"
SUBSET_PREFIX=PMC_SUBSET_
MAX_SUBSET_INDEX=41


AUGMENTED_TEXT_PIPELINE_KEY="DOC_TEXT_AUGMENTATION"
AUGMENTED_TEXT_PIPELINE_VERSION="recent"
OGER_PIPELINE_VERSION="recent"

OUTPUT_PIPELINE_VERSION='0.3.0'

OGER_PP_PIPELINE_VERSION="not_needed_for_PP_part_1"
TARGET_PROCESSING_STATUS_FLAG=OGER_PP1_DONE

# ---------------------------------------------
# process a single collection
#COLLECTION="PUBMED_SUB_37"
#$SCRIPT $PROJECT_ID ${COLLECTION} ${STAGE_LOCATION} ${TEMP_LOCATION} ${WORK_BUCKET} $TEXT_PIPELINE_KEY $TEXT_PIPELINE_VERSION $AUGMENTED_TEXT_PIPELINE_KEY $AUGMENTED_TEXT_PIPELINE_VERSION $OGER_PIPELINE_VERSION $OGER_PP_PIPELINE_VERSION $TARGET_PROCESSING_STATUS_FLAG $JAR_VERSION $OUTPUT_PIPELINE_VERSION &> "./logs/oger-post-process-part1-${COLLECTION}.log" &


# ---------------------------------------------
# use the below for bulk processing
#
for INDEX in $(seq 0 4 $MAX_SUBSET_INDEX)  
  do 
    ind=$(($INDEX + 0))
    if (( ind <= $MAX_SUBSET_INDEX)); then
        echo "Starting oger pipeline... ${ind} $(date)"
        COLLECTION="${SUBSET_PREFIX}${ind}"
        $SCRIPT $PROJECT_ID ${COLLECTION} ${STAGE_LOCATION} ${TEMP_LOCATION} ${WORK_BUCKET} $TEXT_PIPELINE_KEY $TEXT_PIPELINE_VERSION $AUGMENTED_TEXT_PIPELINE_KEY $AUGMENTED_TEXT_PIPELINE_VERSION $OGER_PIPELINE_VERSION $OGER_PP_PIPELINE_VERSION $TARGET_PROCESSING_STATUS_FLAG $JAR_VERSION $OUTPUT_PIPELINE_VERSION &> "./logs/oger-post-process-part1-${COLLECTION}.log" &
        sleep 120
    fi
    ind=$(($INDEX + 1))
    if (( ind <= $MAX_SUBSET_INDEX)); then
        echo "Starting oger pipeline... ${ind} $(date)"
        COLLECTION="${SUBSET_PREFIX}${ind}"
        $SCRIPT $PROJECT_ID ${COLLECTION} ${STAGE_LOCATION} ${TEMP_LOCATION} ${WORK_BUCKET} $TEXT_PIPELINE_KEY $TEXT_PIPELINE_VERSION $AUGMENTED_TEXT_PIPELINE_KEY $AUGMENTED_TEXT_PIPELINE_VERSION $OGER_PIPELINE_VERSION $OGER_PP_PIPELINE_VERSION $TARGET_PROCESSING_STATUS_FLAG $JAR_VERSION $OUTPUT_PIPELINE_VERSION &> "./logs/oger-post-process-part1-${COLLECTION}.log" &
        sleep 120
    fi
    ind=$(($INDEX + 2))
    if (( ind <= $MAX_SUBSET_INDEX)); then
        echo "Starting oger pipeline... ${ind} $(date)"
        COLLECTION="${SUBSET_PREFIX}${ind}"
        $SCRIPT $PROJECT_ID ${COLLECTION} ${STAGE_LOCATION} ${TEMP_LOCATION} ${WORK_BUCKET} $TEXT_PIPELINE_KEY $TEXT_PIPELINE_VERSION $AUGMENTED_TEXT_PIPELINE_KEY $AUGMENTED_TEXT_PIPELINE_VERSION $OGER_PIPELINE_VERSION $OGER_PP_PIPELINE_VERSION $TARGET_PROCESSING_STATUS_FLAG $JAR_VERSION $OUTPUT_PIPELINE_VERSION &> "./logs/oger-post-process-part1-${COLLECTION}.log" &
        sleep 120
    fi
    ind=$(($INDEX + 3))
    if (( ind <= $MAX_SUBSET_INDEX)); then
        echo "Starting oger pipeline... ${ind} $(date)"
        COLLECTION="${SUBSET_PREFIX}${ind}"
        $SCRIPT $PROJECT_ID ${COLLECTION} ${STAGE_LOCATION} ${TEMP_LOCATION} ${WORK_BUCKET} $TEXT_PIPELINE_KEY $TEXT_PIPELINE_VERSION $AUGMENTED_TEXT_PIPELINE_KEY $AUGMENTED_TEXT_PIPELINE_VERSION $OGER_PIPELINE_VERSION $OGER_PP_PIPELINE_VERSION $TARGET_PROCESSING_STATUS_FLAG $JAR_VERSION $OUTPUT_PIPELINE_VERSION &> "./logs/oger-post-process-part1-${COLLECTION}.log" &
    fi
    wait 
  done