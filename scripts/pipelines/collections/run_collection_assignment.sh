#!/bin/sh

# this pipeline assigns collections to status entities that are missing a 
# specified document. It is useful for marking documents for re-processing 
# when there are errors during a pipeline run. All documents that failed can be
# tagged as a single collection, so they can be re-processed in a single run.

INPUT_DOCUMENT_CRITERIA=$1
INPUT_COLLECTION=$2
OUTPUT_COLLECTION=$3
TARGET_PROCESSING_STATUS_FLAG=$4
PROJECT=$5
STAGE_LOCATION=$6
TMP_LOCATION=$7
JAR_VERSION=8

JOB_NAME=$(echo "COLLECTION-ASSIGNMENT-${INPUT_COLLECTION}" | tr '_' '-')

echo "INPUT_DOCUMENT_CRITERIA: $INPUT_DOCUMENT_CRITERIA"
echo "INPUT_COLLECTION: $INPUT_COLLECTION"
echo "OUTPUT_COLLECTION: $OUTPUT_COLLECTION"
echo "TARGET_PROCESSING_FLAG: $TARGET_PROCESSING_STATUS_FLAG"

java -Dfile.encoding=UTF-8 -jar "target/tm-pipelines-bundled-${JAR_VERSION}.jar" COLLECTION_ASSIGNMENT \
--jobName="$JOB_NAME" \
--inputDocumentCriteria="$INPUT_DOCUMENT_CRITERIA" \
--inputCollection="$INPUT_COLLECTION" \
--outputCollection="$OUTPUT_COLLECTION" \
--targetProcessingStatusFlag="$TARGET_PROCESSING_STATUS_FLAG" \
--project="${PROJECT}" \
--stagingLocation="$STAGE_LOCATION" \
--gcpTempLocation="$TMP_LOCATION" \
--workerZone=us-central1-c \
--region=us-central1 \
--numWorkers=10 \
--maxNumWorkers=50 \
--workerMachineType=n1-highmem-2 \
--autoscalingAlgorithm=THROUGHPUT_BASED \
--runner=DataflowRunner