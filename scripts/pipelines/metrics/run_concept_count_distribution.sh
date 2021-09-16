#!/bin/sh

PROJECT=$1
STAGE_LOCATION=$2
TMP_LOCATION=$3
BUCKET=$4

JOB_NAME='CONCEPT-COUNT-DISTRIBUTION'

SINGLETON_FILE_PATTERN="${BUCKET}/output/concept-counts/concept-to-doc.*"
LABEL_MAP_FILE="${BUCKET}/ontology-resources/ontology-class-label-map.tsv.gz"
OUTPUT_BUCKET="${BUCKET}/output/concept-count-distribution/concept-count-distribution-"

echo "COLLECTION: $COLLECTION"
echo "PROJECT: $PROJECT"
echo "JOB_NAME: $JOB_NAME"

java -Dfile.encoding=UTF-8 -jar target/tm-pipelines-bundled-0.1.0.jar CONCEPT_COUNT_DISTRIBUTION \
--jobName="$JOB_NAME" \
--singletonFilePattern="$SINGLETON_FILE_PATTERN" \
--labelMapFilePattern="$LABEL_MAP_FILE" \
--labelMapFileDelimiter='TAB' \
--outputBucket="$OUTPUT_BUCKET" \
--project="${PROJECT}" \
--stagingLocation="$STAGE_LOCATION" \
--gcpTempLocation="$TMP_LOCATION" \
--workerZone=us-central1-c \
--region=us-central1 \
--numWorkers=10 \
--maxNumWorkers=200 \
--runner=DataflowRunner