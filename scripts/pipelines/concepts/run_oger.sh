#!/bin/sh

SERVICE_URL=$1
ONT=$2
PROJECT=$3
COLLECTION=$4
STAGE_LOCATION=$5
TMP_LOCATION=$6
TEXT_PIPELINE_KEY=$7
TEXT_PIPELINE_VERSION=$8
OUTPUT_PIPELINE_VERSION=$9
OVERWRITE=${10}
JAR_VERSION=${11}


TPSF="OGER_${ONT}_DONE"
TDT="CONCEPT_${ONT}"
JOB_NAME=$(echo "OGER-${ONT}-${COLLECTION}-${TEXT_PIPELINE_KEY}" | tr '_' '-')


echo "SERVICE URL: $SERVICE_URL"
echo "ONT: $ONT"
echo "COLLECTION: $COLLECTION"
echo "PROJECT: $PROJECT"
echo "TPSF: $TPSF"
echo "TDT: $TDT"
echo "JOB_NAME: $JOB_NAME"
echo "TEXT PIPELINE KEY: $TEXT_PIPELINE_KEY"
echo "TEXT PIPELINE VERSION: $TEXT_PIPELINE_VERSION"
echo "OUTPUT_PIPELINE_VERSION KEY: $OUTPUT_PIPELINE_VERSION"
echo "OVERWRITE KEY: $OVERWRITE"

java -Dfile.encoding=UTF-8 -jar "target/tm-pipelines-bundled-${JAR_VERSION}.jar" OGER \
--jobName="$JOB_NAME" \
--ogerServiceUri="$SERVICE_URL" \
--ogerOutputType=TSV \
--targetProcessingStatusFlag="$TPSF" \
--targetDocumentType="$TDT" \
--targetDocumentFormat="BIONLP" \
--inputPipelineKey="$TEXT_PIPELINE_KEY" \
--inputPipelineVersion="$TEXT_PIPELINE_VERSION" \
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