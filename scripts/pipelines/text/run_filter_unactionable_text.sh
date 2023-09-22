#!/bin/sh

TEXT_PIPELINE_KEY=$1
TEXT_PIPELINE_VERSION=$2
OUTPUT_PIPELINE_VERSION=$3
OUTPUT_BUCKET=$4
COLLECTION=$5
OVERWRITE=$6
PROJECT=$7
STAGE_LOCATION=$8
TMP_LOCATION=$9
JAR_VERSION=${10}

JOB_NAME=$(echo "FILTER_UNACTIONABLE_TEXT" | tr '_' '-')

echo "TEXT_PIPELINE_KEY: $TEXT_PIPELINE_KEY"
echo "TEXT_PIPELINE_VERSION: $TEXT_PIPELINE_VERSION"
echo "OUTPUT_PIPELINE_VERSION: $OUTPUT_PIPELINE_VERSION"
echo "OUTPUT_BUCKET: $OUTPUT_BUCKET"
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
--outputBucket="$OUTPUT_BUCKET" \
--overwrite="$OVERWRITE" \
--collection="$COLLECTION" \
--project="${PROJECT}" \
--stagingLocation="$STAGE_LOCATION" \
--gcpTempLocation="$TMP_LOCATION" \
--workerZone=us-central1-c \
--region=us-central1 \
--numWorkers=10 \
--maxNumWorkers=200 \
--autoscalingAlgorithm=THROUGHPUT_BASED \
--defaultWorkerLogLevel=INFO \
--runner=DataflowRunner
