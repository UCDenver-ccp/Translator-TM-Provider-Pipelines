#!/bin/sh

MEDLINE_XML_DIR=$1
PMID_SKIP_FILE_PATH=$2
COLLECTION=$3
OVERWRITE=$4
PROJECT=$5
STAGE_LOCATION=$6
TMP_LOCATION=$7

JOB_NAME=$(echo "MEDLINE_XML_TO_TEXT" | tr '_' '-')

echo "MEDLINE_XML_DIR: $MEDLINE_XML_DIR"
echo "PMID_SKIP_FILE_PATH: $PMID_SKIP_FILE_PATH"
echo "COLLECTION: $COLLECTION"
echo "OVERWRITE: $OVERWRITE"
echo "PROJECT: $PROJECT"
echo "JOB_NAME: $JOB_NAME"

java -jar target/tm-pipelines-bundled-0.1.0.jar MEDLINE_XML_TO_TEXT \
--jobName="$JOB_NAME" \
--medlineXmlDir="$MEDLINE_XML_DIR" \
--pmidSkipFilePath="$PMID_SKIP_FILE_PATH" \
--overwrite="$OVERWRITE" \
--collection="$COLLECTION" \
--project="${PROJECT}" \
--stagingLocation="$STAGE_LOCATION" \
--gcpTempLocation="$TMP_LOCATION" \
--workerZone=us-central1-c \
--region=us-central1 \
--workerMachineType=n1-standard-16 \
--numWorkers=10 \
--maxNumWorkers=200 \
--defaultWorkerLogLevel=INFO \
--runner=DataflowRunner
