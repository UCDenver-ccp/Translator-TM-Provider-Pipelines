#!/bin/sh
# this parses the dependency parse conllu file and creates a documents that contains just sentence annotations.

PROJECT=$1
COLLECTION=$2
STAGE_LOCATION=$3
TMP_LOCATION=$4
TEXT_PIPELINE_KEY=$5
TEXT_PIPELINE_VERSION=$6
OUTPUT_SENTENCE_VERSION=$7
DP_PIPELINE_VERSION=$8
JAR_VERSION=${9}
OVERWRITE=${10}

JOB_NAME=$(echo "DP_TO_SENT-${COLLECTION}-${TEXT_PIPELINE_KEY}" | tr '_' '-')

echo "COLLECTION: $COLLECTION"
echo "PROJECT: $PROJECT"
echo "JOB_NAME: $JOB_NAME"
echo "TEXT PIPELINE KEY: $TEXT_PIPELINE_KEY"
echo "TEXT PIPELINE VERSION: $TEXT_PIPELINE_VERSION"
echo "DEPENDENCY PARSE PIPELINE VERSION: $DP_PIPELINE_VERSION"
echo "OUTPUT_SENTENCE_VERSION KEY: $OUTPUT_SENTENCE_VERSION"
echo "OVERWRITE KEY: $OVERWRITE"

java -Dfile.encoding=UTF-8 -jar "target/tm-pipelines-bundled-${JAR_VERSION}.jar" DEPENDENCY_PARSE_TO_SENTENCE \
--jobName="$JOB_NAME" \
--textPipelineKey="$TEXT_PIPELINE_KEY" \
--textPipelineVersion="$TEXT_PIPELINE_VERSION" \
--dpPipelineVersion="$DP_PIPELINE_VERSION" \
--outputSentenceVersion="$OUTPUT_SENTENCE_VERSION" \
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