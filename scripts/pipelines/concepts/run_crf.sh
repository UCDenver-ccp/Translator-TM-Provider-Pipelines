#!/bin/sh

SERVICE_URL=$1
ONT=$2
SENTENCE_PIPELINE_KEY=$3
SENTENCE_PIPELINE_VERSION=$4
AUGMENTED_SENTENCE_PIPELINE_KEY=$5
AUGMENTED_SENTENCE_PIPELINE_VERSION=$6
PROJECT=$7
COLLECTION=$8
OVERWRITE=$9
STAGE_LOCATION=${10}
TMP_LOCATION=${11}
OUTPUT_PIPELINE_VERSION=${12}
JAR_VERSION=${13}


TPSF="CRF_${ONT}_DONE"
TDT="CRF_${ONT}"
JOB_NAME=$(echo "CRF-${ONT}-${COLLECTION}" | tr '_' '-')


echo "SERVICE URL: $SERVICE_URL"
echo "ONT: $ONT"
echo "COLLECTION: $COLLECTION"
echo "PROJECT: $PROJECT"
echo "TPSF: $TPSF"
echo "TDT: $TDT"
echo "JOB_NAME: $JOB_NAME"

java -Dfile.encoding=UTF-8 -jar "target/tm-pipelines-bundled-${JAR_VERSION}.jar" CRF \
--jobName="$JOB_NAME" \
--crfServiceUri="$SERVICE_URL" \
--targetProcessingStatusFlag="$TPSF" \
--targetDocumentType="$TDT" \
--inputSentencePipelineKey="$SENTENCE_PIPELINE_KEY" \
--inputSentencePipelineVersion="$SENTENCE_PIPELINE_VERSION" \
--augmentedSentencePipelineKey="$AUGMENTED_SENTENCE_PIPELINE_KEY" \
--augmentedSentencePipelineVersion="$AUGMENTED_SENTENCE_PIPELINE_VERSION" \
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
--runner=DataflowRunner