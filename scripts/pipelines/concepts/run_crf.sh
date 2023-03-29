#!/bin/sh

SERVICE_URL=$1
ONT=$2
PROJECT=$3
COLLECTION=$4
STAGE_LOCATION=$5
TMP_LOCATION=$6


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

java -Dfile.encoding=UTF-8 -jar target/tm-pipelines-bundled-0.1.0.jar CRF \
--jobName="$JOB_NAME" \
--crfServiceUri="$SERVICE_URL" \
--targetProcessingStatusFlag="$TPSF" \
--targetDocumentType="$TDT" \
--inputSentencePipelineKey='SENTENCE_SEGMENTATION' \
--inputSentencePipelineVersion='0.1.0' \
--collection="$COLLECTION" \
--overwrite='NO' \
--project="${PROJECT}" \
--stagingLocation="$STAGE_LOCATION" \
--gcpTempLocation="$TMP_LOCATION" \
--workerZone=us-central1-c \
--region=us-central1 \
--numWorkers=10 \
--maxNumWorkers=50 \
--autoscalingAlgorithm=THROUGHPUT_BASED \
--runner=DataflowRunner