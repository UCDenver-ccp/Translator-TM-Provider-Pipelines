#!/bin/sh

PROJECT=$1
COLLECTION=$2
STAGE_LOCATION=$3
TMP_LOCATION=$4
PIPELINE_KEY=$5


JOB_NAME=$(echo "SENTENCE-${COLLECTION}" | tr '_' '-')


echo "COLLECTION: $COLLECTION"
echo "PROJECT: $PROJECT"
echo "JOB_NAME: $JOB_NAME"

java -Dfile.encoding=UTF-8 -jar target/tm-pipelines-bundled-0.1.0.jar SENTENCE_SEGMENTATION \
--jobName="$JOB_NAME" \
--inputPipelineKey="$PIPELINE_KEY" \
--inputPipelineVersion='0.1.0' \
--collection="$COLLECTION" \
--overwrite='NO' \
--project="${PROJECT}" \
--stagingLocation="$STAGE_LOCATION" \
--gcpTempLocation="$TMP_LOCATION" \
--workerZone=us-central1-c \
--region=us-central1 \
--numWorkers=10 \
--maxNumWorkers=50 \
--autoscalingAlgorithm=THROUGHPUT_BASED \
--workerMachineType=n1-highmem-2 \
--runner=DataflowRunner