#!/bin/sh

PROJECT=$1
COLLECTION=$2
STAGE_LOCATION=$3
TMP_LOCATION=$4


JOB_NAME=$(echo "SENTENCE-${COLLECTION}" | tr '_' '-')


echo "COLLECTION: $COLLECTION"
echo "PROJECT: $PROJECT"
echo "JOB_NAME: $JOB_NAME"

java -Dfile.encoding=UTF-8 -jar target/tm-pipelines-bundled-0.1.0.jar SENTENCE_SEGMENTATION \
--jobName=$JOB_NAME \
--inputPipelineKey='MEDLINE_XML_TO_TEXT' \
--inputPipelineVersion='0.1.0' \
--collection=$COLLECTION \
--overwrite='YES' \
--queryLimit=0 \
--project=${PROJECT} \
--stagingLocation=$STAGE_LOCATION \
--gcpTempLocation=$TMP_LOCATION \
--zone=us-central1-c \
--numWorkers=10 \
--maxNumWorkers=25 \
--workerMachineType=n1-highmem-2 \
--runner=DataflowRunner