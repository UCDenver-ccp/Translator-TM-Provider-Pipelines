#!/bin/sh

TEXT_PIPELINE_KEY=$1
TEXT_PIPELINE_VERSION=$2
OUTPUT_PIPELINE_VERSION=$3
#OUTPUT_BUCKET=$4
COLLECTION=$4
OVERWRITE=$5
PROJECT=$6
STAGE_LOCATION=$7
TMP_LOCATION=$8
JAR_VERSION=$9

JOB_NAME=$(echo "FILTER-UNACTIONABLE-TEXT-${COLLECTION}" | tr '_' '-')

echo "TEXT_PIPELINE_KEY: $TEXT_PIPELINE_KEY"
echo "TEXT_PIPELINE_VERSION: $TEXT_PIPELINE_VERSION"
echo "OUTPUT_PIPELINE_VERSION: $OUTPUT_PIPELINE_VERSION"
#echo "OUTPUT_BUCKET: $OUTPUT_BUCKET"
echo "JAR_VERSION: $JAR_VERSION"
echo "COLLECTION: $COLLECTION"
echo "OVERWRITE: $OVERWRITE"
echo "PROJECT: $PROJECT"
echo "JOB_NAME: $JOB_NAME"

java -jar "target/tm-pipelines-bundled-${JAR_VERSION}.jar" FILTER_UNACTIONABLE_TEXT \
--jobName="$JOB_NAME" \
--textPipelineKey="$TEXT_PIPELINE_KEY" \
--textPipelineVersion="$TEXT_PIPELINE_VERSION" \
--outputPipelineVersion="$OUTPUT_PIPELINE_VERSION" \
--overwrite="$OVERWRITE" \
--collection="$COLLECTION" \
--project="${PROJECT}" \
--stagingLocation="$STAGE_LOCATION" \
--gcpTempLocation="$TMP_LOCATION" \
--workerZone=us-central1-c \
--region=us-central1 \
--numWorkers=10 \
--maxNumWorkers=50 \
--workerMachineType=n1-highmem-2 \
--autoscalingAlgorithm=THROUGHPUT_BASED \
--defaultWorkerLogLevel=INFO \
--runner=DataflowRunner
