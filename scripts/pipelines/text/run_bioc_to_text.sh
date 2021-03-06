#!/bin/sh

BIOC_DIR=$1
PROJECT=$2
COLLECTION=$3
STAGE_LOCATION=$4
TMP_LOCATION=$5

JOB_NAME=$(echo "BIOC_TO_TEXT" | tr '_' '-')

echo "BIOC_DIR: $BIOC_DIR"
echo "COLLECTION: $COLLECTION"
echo "PROJECT: $PROJECT"
echo "JOB_NAME: $JOB_NAME"

java -Dfile.encoding=UTF-8 -jar target/tm-pipelines-bundled-0.1.0.jar BIOC_TO_TEXT \
--jobName="$JOB_NAME" \
--biocDir="$BIOC_DIR" \
--collection="$COLLECTION" \
--project="${PROJECT}" \
--stagingLocation="$STAGE_LOCATION" \
--gcpTempLocation="$TMP_LOCATION" \
--zone=us-central1-c \
--numWorkers=10 \
--maxNumWorkers=200 \
--runner=DataflowRunner