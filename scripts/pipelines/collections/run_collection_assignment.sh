#!/bin/sh

# this pipeline assigns collections to status entities that are missing a 
# specified document. It is useful for marking documents for re-processing 
# when there are errors during a pipeline run. All documents that failed can be
# tagged as a single collection, so they can be re-processed in a single run.

INPUT_DOCUMENT_TYPE=$1
INPUT_DOCUMENT_FORMAT=$2
INPUT_PIPELINE_KEY=$3
INPUT_PIPELINE_VERSION=$4
INPUT_COLLECTION=$5
OUTPUT_COLLECTION=$6
TARGET_PROCESSING_STATUS_FLAG=$7
PROJECT=$8
STAGE_LOCATION=$9
TMP_LOCATION=${10}
JAR_VERSION=${11}

JOB_NAME=$(echo "COLLECTION-ASSIGNMENT-${INPUT_COLLECTION}" | tr '_' '-')

echo "INPUT_DOCUMENT_TYPE: $INPUT_DOCUMENT_TYPE"
echo "INPUT_DOCUMENT_FORMAT: $INPUT_DOCUMENT_FORMAT"
echo "INPUT_PIPELINE_KEY: $INPUT_PIPELINE_KEY"
echo "INPUT_PIPELINE_VERSION: $INPUT_PIPELINE_VERSION"
echo "INPUT_COLLECTION: $INPUT_COLLECTION"
echo "OUTPUT_COLLECTION: $OUTPUT_COLLECTION"
echo "TARGET_PROCESSING_FLAG: $TARGET_PROCESSING_STATUS_FLAG"

java -Dfile.encoding=UTF-8 -jar "target/tm-pipelines-bundled-${JAR_VERSION}.jar" COLLECTION_ASSIGNMENT \
--jobName="$JOB_NAME" \
--inputDocumentType="$INPUT_DOCUMENT_TYPE" \
--inputDocumentFormat="$INPUT_DOCUMENT_FORMAT" \
--inputPipelineKey="$INPUT_PIPELINE_KEY" \
--inputPipelineVersion="$INPUT_PIPELINE_VERSION" \
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