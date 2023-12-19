#!/bin/bash

# This script is to be used for text collections such as PMCOA or BioXriv. It uses section information to remove
# text that we don't want to process (unactionable text) such as the references section.

source ./rrun.env.sh

########## PMC
TEXT_PIPELINE_KEY=BIOC_TO_TEXT
SUBSET_PREFIX=PMC_SUBSET_
TEXT_PIPELINE_VERSION="recent"
MAX_SUBSET_INDEX=41

#OUTPUT_BUCKET="$WORK_BUCKET/output/top-level-section-names/tlsn"

SCRIPT=./scripts/pipelines/text/run_filter_unactionable_text.sh
OUTPUT_PIPELINE_VERSION="0.1.0"
OVERWRITE="YES"

COLLECTION="2023-11-26"
echo "Starting filter_unactionable_text pipeline... $COLLECTION $(date)"
$SCRIPT "${TEXT_PIPELINE_KEY}" "${TEXT_PIPELINE_VERSION}" "${OUTPUT_PIPELINE_VERSION}" "${COLLECTION}" "${OVERWRITE}" "${PROJECT_ID}" "${STAGE_LOCATION}" "${TEMP_LOCATION}" "${JAR_VERSION}" &> "./logs/filter-unactionable-text-${COLLECTION}.log" &

# for INDEX in $(seq 0 4 $MAX_SUBSET_INDEX)  
# do 
# ind=$(($INDEX + 0))
# if (( ind <= $MAX_SUBSET_INDEX)); then
#     echo "Starting filter_unactionable_text pipeline... ${ind} $(date)"
#     COLLECTION="${SUBSET_PREFIX}${ind}"
#     $SCRIPT "${TEXT_PIPELINE_KEY}" "${TEXT_PIPELINE_VERSION}" "${OUTPUT_PIPELINE_VERSION}" "${COLLECTION}" "${OVERWRITE}" "${PROJECT_ID}" "${STAGE_LOCATION}" "${TEMP_LOCATION}" "${JAR_VERSION}" &> "./logs/filter-unactionable-text-${COLLECTION}.log" &
#     sleep 120
# fi
# ind=$(($INDEX + 1))
# if (( ind <= $MAX_SUBSET_INDEX)); then
#     echo "Starting filter_unactionable_text pipeline... ${ind} $(date)"
#     COLLECTION="${SUBSET_PREFIX}${ind}"
#     $SCRIPT "${TEXT_PIPELINE_KEY}" "${TEXT_PIPELINE_VERSION}" "${OUTPUT_PIPELINE_VERSION}" "${COLLECTION}" "${OVERWRITE}" "${PROJECT_ID}" "${STAGE_LOCATION}" "${TEMP_LOCATION}" "${JAR_VERSION}" &> "./logs/filter-unactionable-text-${COLLECTION}.log" &
#     sleep 120
# fi
# ind=$(($INDEX + 2))
# if (( ind <= $MAX_SUBSET_INDEX)); then
#     echo "Starting filter_unactionable_text pipeline... ${ind} $(date)"
#     COLLECTION="${SUBSET_PREFIX}${ind}"
#     $SCRIPT "${TEXT_PIPELINE_KEY}" "${TEXT_PIPELINE_VERSION}" "${OUTPUT_PIPELINE_VERSION}" "${COLLECTION}" "${OVERWRITE}" "${PROJECT_ID}" "${STAGE_LOCATION}" "${TEMP_LOCATION}" "${JAR_VERSION}" &> "./logs/filter-unactionable-text-${COLLECTION}.log" &
#     sleep 120
# fi
# ind=$(($INDEX + 3))
# if (( ind <= $MAX_SUBSET_INDEX)); then
#     echo "Starting filter_unactionable_text pipeline... ${ind} $(date)"
#     COLLECTION="${SUBSET_PREFIX}${ind}"
#     $SCRIPT "${TEXT_PIPELINE_KEY}" "${TEXT_PIPELINE_VERSION}" "${OUTPUT_PIPELINE_VERSION}" "${COLLECTION}" "${OVERWRITE}" "${PROJECT_ID}" "${STAGE_LOCATION}" "${TEMP_LOCATION}" "${JAR_VERSION}" &> "./logs/filter-unactionable-text-${COLLECTION}.log" &
# fi
# wait 
# done