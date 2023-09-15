#!/usr/local/bin/bash

source ./rrun.env.sh

SCRIPT=./scripts/pipelines/collections/run_collection_assignment.sh

INPUT_DOCUMENT_TYPE=CRF_CRAFT
INPUT_DOCUMENT_FORMAT=BIONLP
INPUT_PIPELINE_KEY=CRF
INPUT_COLLECTION=TEST
TARGET_PROCESSING_STATUS_FLAG=CRF_DONE

INPUT_PIPELINE_VERSION="0.3.0"
OUTPUT_COLLECTION=MY_TTEST_REDO_CRF_20230914

$SCRIPT $INPUT_DOCUMENT_TYPE $INPUT_DOCUMENT_FORMAT $INPUT_PIPELINE_KEY $INPUT_PIPELINE_VERSION $INPUT_COLLECTION $OUTPUT_COLLECTION $TARGET_PROCESSING_STATUS_FLAG $PROJECT_ID ${STAGE_LOCATION} ${TEMP_LOCATION} $JAR_VERSION &> "./logs/collection-assignment-${OUTPUT_COLLECTION}.log" &