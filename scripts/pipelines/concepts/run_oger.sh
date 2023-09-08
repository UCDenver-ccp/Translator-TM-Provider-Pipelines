#!/bin/sh

CS_SERVICE_URL=$1
CIMIN_SERVICE_URL=$2
CIMAX_SERVICE_URL=$3
PROJECT=$4
COLLECTION=$5
STAGE_LOCATION=$6
TMP_LOCATION=$7
TEXT_PIPELINE_KEY=$8
TEXT_PIPELINE_VERSION=$9
AUGMENTED_TEXT_PIPELINE_KEY=${10}
AUGMENTED_TEXT_PIPELINE_VERSION=${11}
OUTPUT_PIPELINE_VERSION=${12}
OVERWRITE=${13}
JAR_VERSION=${14}


# TPSF="OGER_${ONT}_DONE"
# TDT="CONCEPT_${ONT}"
# JOB_NAME=$(echo "OGER-${ONT}-${COLLECTION}-${TEXT_PIPELINE_KEY}" | tr '_' '-')
JOB_NAME=$(echo "OGER-${COLLECTION}" | tr '_' '-')

echo "CS SERVICE URL: $CS_SERVICE_URL"
echo "CIMIN SERVICE URL: $CIMIN_SERVICE_URL"
echo "CIMAX SERVICE URL: $CIMAX_SERVICE_URL"
echo "COLLECTION: $COLLECTION"
echo "PROJECT: $PROJECT"
# echo "TPSF: $TPSF"
# echo "TDT: $TDT"
echo "JOB_NAME: $JOB_NAME"
echo "TEXT PIPELINE KEY: $TEXT_PIPELINE_KEY"
echo "TEXT PIPELINE VERSION: $TEXT_PIPELINE_VERSION"
echo "AUGMENTED TEXT PIPELINE KEY: $AUGMENTED_TEXT_PIPELINE_KEY"
echo "AUGMENTED TEXT PIPELINE VERSION: $AUGMENTED_TEXT_PIPELINE_VERSION"
echo "OUTPUT_PIPELINE_VERSION KEY: $OUTPUT_PIPELINE_VERSION"
echo "OVERWRITE KEY: $OVERWRITE"
echo "JAR_VERSION: $JAR_VERSION"

java -Dfile.encoding=UTF-8 -jar "target/tm-pipelines-bundled-${JAR_VERSION}.jar" OGER \
--jobName="$JOB_NAME" \
--csOgerServiceUri="$CS_SERVICE_URL" \
--ciminOgerServiceUri="$CIMIN_SERVICE_URL" \
--cimaxOgerServiceUri="$CIMAX_SERVICE_URL" \
--textPipelineKey="$TEXT_PIPELINE_KEY" \
--textPipelineVersion="$TEXT_PIPELINE_VERSION" \
--augmentedTextPipelineKey="$AUGMENTED_TEXT_PIPELINE_KEY" \
--augmentedTextPipelineVersion="$AUGMENTED_TEXT_PIPELINE_VERSION" \
--outputPipelineVersion="$OUTPUT_PIPELINE_VERSION" \
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