#!/bin/bash

# This script is to be used for text collections such as PMCOA or BioXriv. It uses section information to remove
# text that we don't want to process (unactionable text) such as the references section.

source ./rrun.env.sh

########## PMC
TEXT_PIPELINE_KEY=BIOC_TO_TEXT
SUBSET_PREFIX=PMC_SUBSET_
TEXT_PIPELINE_VERSION="recent"

OUTPUT_BUCKET="$WORK_BUCKET/output/top-level-section-names/tlsn"

SCRIPT=./scripts/pipelines/text/run_filter_unactionable_text.sh
OUTPUT_PIPELINE_VERSION="0.1.0"
OVERWRITE="YES"
JAR_VERSION="0.2.1"

COLLECTION="PMC_SUBSET_28"

echo "Starting filter_unactionable_text pipeline... $COLLECTION $(date)"
$SCRIPT "${TEXT_PIPELINE_KEY}" "${TEXT_PIPELINE_VERSION}" "${OUTPUT_PIPELINE_VERSION}" "${OUTPUT_BUCKET}" "${COLLECTION}" "${OVERWRITE}" "${PROJECT_ID}" "${STAGE_LOCATION}" "${TEMP_LOCATION}" "${JAR_VERSION}" &> "./logs/filter-unactionable-text-${COLLECTION}.log" &

