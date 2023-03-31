#!/bin/sh

PROJECT=$1
COLLECTION=$2
TEXT_PIPELINE_KEY=$3
OVERWRITE_FLAG=$4
OUTPUT_BUCKET=$5
STAGE_LOCATION=$6
TMP_LOCATION=$7
JAR_VERSION=$8

ZONE='us-central1-c'
REGION='us-central1'
JOB_NAME=$(echo "DOC-TEXT-EXPORT-${COLLECTION}" | tr '_' '-')


echo "COLLECTION: $COLLECTION"
echo "PROJECT: $PROJECT"
echo "TEXT_PIPELINE_KEY: $TEXT_PIPELINE_KEY"
echo "OVERWRITE_FLAG: $OVERWRITE_FLAG"
echo "OUTPUT_BUCKET: $OUTPUT_BUCKET"
echo "JOB_NAME: $JOB_NAME"

java -Dfile.encoding=UTF-8 -jar target/tm-pipelines-bundled-${JAR_VERSION}.jar TEXT_EXPORT \
--jobName="$JOB_NAME" \
--inputTextPipelineKey="$TEXT_PIPELINE_KEY" \
--inputTextPipelineVersion="0.1.0" \
--collection="$COLLECTION" \
--outputBucket="$OUTPUT_BUCKET" \
--overwrite="$OVERWRITE_FLAG" \
--project="${PROJECT}" \
--stagingLocation="$STAGE_LOCATION" \
--gcpTempLocation="$TMP_LOCATION" \
--workerZone="$ZONE" \
--region="$REGION" \
--numWorkers=10 \
--maxNumWorkers=200 \
--autoscalingAlgorithm=THROUGHPUT_BASED \
--runner=DataflowRunner