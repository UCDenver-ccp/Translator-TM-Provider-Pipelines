#!/bin/sh

CRAFT_SERVICE_URL=$1
NLMDISEASE_SERVICE_URL=$2
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
OPTIONAL_DOCUMENT_SPECIFIC_COLLECTION=${13}
JAR_VERSION=${14}

JOB_NAME=$(echo "CRF-${COLLECTION}" | tr '_' '-')

echo "CRAFT SERVICE URL: $CRAFT_SERVICE_URL"
echo "NLM DISEASE SERVICE URL: $NLMDISEASE_SERVICE_URL"
echo "COLLECTION: $COLLECTION"
echo "PROJECT: $PROJECT"
echo "JOB_NAME: $JOB_NAME"
echo "OPTIONAL_DOCUMENT_SPECIFIC_COLLECTION: $OPTIONAL_DOCUMENT_SPECIFIC_COLLECTION"

java -Dfile.encoding=UTF-8 -jar "target/tm-pipelines-bundled-${JAR_VERSION}.jar" CRF \
--jobName="$JOB_NAME" \
--craftCrfServiceUri="$CRAFT_SERVICE_URL" \
--nlmDiseaseCrfServiceUri="$NLMDISEASE_SERVICE_URL" \
--inputSentencePipelineKey="$SENTENCE_PIPELINE_KEY" \
--inputSentencePipelineVersion="$SENTENCE_PIPELINE_VERSION" \
--augmentedSentencePipelineKey="$AUGMENTED_SENTENCE_PIPELINE_KEY" \
--augmentedSentencePipelineVersion="$AUGMENTED_SENTENCE_PIPELINE_VERSION" \
--outputPipelineVersion="$OUTPUT_PIPELINE_VERSION" \
--optionalDocumentSpecificCollection="$OPTIONAL_DOCUMENT_SPECIFIC_COLLECTION" \
--collection="$COLLECTION" \
--overwrite="$OVERWRITE" \
--project="${PROJECT}" \
--stagingLocation="$STAGE_LOCATION" \
--gcpTempLocation="$TMP_LOCATION" \
--workerZone=us-central1-c \
--region=us-central1 \
--numWorkers=10 \
--maxNumWorkers=50 \
--workerMachineType=n1-highmem-2 \
--autoscalingAlgorithm=THROUGHPUT_BASED \
--runner=DataflowRunner