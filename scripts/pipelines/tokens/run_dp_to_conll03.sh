#!/bin/sh
# this parses the dependency parse conllu file and creates a file of tokens in the CoNLL '03 file format

PROJECT=$1
COLLECTION=$2
STAGE_LOCATION=$3
TMP_LOCATION=$4
TEXT_PIPELINE_KEY=$5
TEXT_PIPELINE_VERSION=$6
DP_PIPELINE_VERSION=$7
OUTPUT_BUCKET=$8
JAR_VERSION=${9}
OVERWRITE=${10}

JOB_NAME=$(echo "DP_TO_CONLL03-${COLLECTION}-${TEXT_PIPELINE_KEY}" | tr '_' '-')

echo "COLLECTION: $COLLECTION"
echo "PROJECT: $PROJECT"
echo "JOB_NAME: $JOB_NAME"
echo "TEXT PIPELINE KEY: $TEXT_PIPELINE_KEY"
echo "TEXT PIPELINE VERSION: $TEXT_PIPELINE_VERSION"
echo "DEPENDENCY PARSE PIPELINE VERSION: $DP_PIPELINE_VERSION"
echo "OUTPUT_BUCKET: $OUTPUT_BUCKET"
echo "OVERWRITE KEY: $OVERWRITE"

java -Dfile.encoding=UTF-8 -jar "target/tm-pipelines-bundled-${JAR_VERSION}.jar" DEPENDENCY_PARSE_TO_CONLL03 \
--jobName="$JOB_NAME" \
--textPipelineKey="$TEXT_PIPELINE_KEY" \
--textPipelineVersion="$TEXT_PIPELINE_VERSION" \
--dpPipelineVersion="$DP_PIPELINE_VERSION" \
--outputBucket="$OUTPUT_BUCKET" \
--collection="$COLLECTION" \
--overwrite="$OVERWRITE" \
--project="${PROJECT}" \
--stagingLocation="$STAGE_LOCATION" \
--gcpTempLocation="$TMP_LOCATION" \
--workerZone=us-central1-c \
--region=us-central1 \
--numWorkers=10 \
--maxNumWorkers=50 \
--autoscalingAlgorithm=THROUGHPUT_BASED \
--defaultWorkerLogLevel=INFO \
--runner=DataflowRunner