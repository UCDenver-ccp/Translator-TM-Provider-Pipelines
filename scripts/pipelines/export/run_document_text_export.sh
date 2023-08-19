#!/bin/sh

PROJECT=$1
COLLECTION=$2
TEXT_PIPELINE_KEY=$3
TEXT_PIPELINE_VERSION=$4
OVERWRITE_FLAG=$5
OUTPUT_BUCKET=$6
STAGE_LOCATION=$7
TMP_LOCATION=$8
JAR_VERSION=$9

ZONE='us-central1-c'
REGION='us-central1'
JOB_NAME=$(echo "DOC-TEXT-EXPORT-${COLLECTION}" | tr '_' '-')


echo "COLLECTION: $COLLECTION"
echo "PROJECT: $PROJECT"
echo "TEXT_PIPELINE_KEY: $TEXT_PIPELINE_KEY"
echo "OVERWRITE_FLAG: $OVERWRITE_FLAG"
echo "OUTPUT_BUCKET: $OUTPUT_BUCKET"
echo "JOB_NAME: $JOB_NAME"

java -Dfile.encoding=UTF-8 -jar "target/tm-pipelines-bundled-${JAR_VERSION}.jar" TEXT_EXPORT \
--jobName="$JOB_NAME" \
--inputTextPipelineKey="$TEXT_PIPELINE_KEY" \
--inputTextPipelineVersion="$TEXT_PIPELINE_VERSION" \
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