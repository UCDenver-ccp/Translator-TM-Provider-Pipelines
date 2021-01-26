#!/bin/sh

BIOC_DIR=$1
PROJECT=$2
COLLECTION=$3
OVERWRITE=$4
STAGE_LOCATION=$5
TMP_LOCATION=$6

JOB_NAME=$(echo "BIOC_TO_TEXT" | tr '_' '-')

echo "BIOC_DIR: $BIOC_DIR"
echo "COLLECTION: $COLLECTION"
echo "OVERWRITE: $OVERWRITE"
echo "PROJECT: $PROJECT"
echo "JOB_NAME: $JOB_NAME"

java -jar target/tm-pipelines-bundled-0.1.0.jar BIOC_TO_TEXT \
--jobName="$JOB_NAME" \
--biocDir="$BIOC_DIR" \
--overwrite="$OVERWRITE" \
--collection="$COLLECTION" \
--project="${PROJECT}" \
--stagingLocation="$STAGE_LOCATION" \
--gcpTempLocation="$TMP_LOCATION" \
--workerZone=us-central1-c \
--region=us-central1 \
--numWorkers=10 \
--maxNumWorkers=200 \
--defaultWorkerLogLevel=DEBUG \
--runner=DataflowRunner
