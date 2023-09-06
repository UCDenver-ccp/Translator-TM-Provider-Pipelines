#!/bin/sh

PROJECT=$1
COLLECTION=$2
STAGE_LOCATION=$3
TMP_LOCATION=$4
TEXT_PIPELINE_KEY=$5
TEXT_PIPELINE_VERSION=$6
OVERWRITE=$7
JAR_VERSION=$8


JOB_NAME=$(echo "SENTENCE-${COLLECTION}" | tr '_' '-')


echo "COLLECTION: $COLLECTION"
echo "PROJECT: $PROJECT"
echo "JOB_NAME: $JOB_NAME"

java -Dfile.encoding=UTF-8 -jar target/tm-pipelines-bundled-${JAR_VERSION}.jar SENTENCE_SEGMENTATION \
--jobName="$JOB_NAME" \
--inputPipelineKey="$TEXT_PIPELINE_KEY" \
--inputPipelineVersion="${TEXT_PIPELINE_VERSION}" \
--collection="$COLLECTION" \
--overwrite="${OVERWRITE}" \
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