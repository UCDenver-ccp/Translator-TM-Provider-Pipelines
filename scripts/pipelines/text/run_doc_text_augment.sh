#!/bin/sh
# this creates a version of the document text that has sentences appended at the bottom related to abbreviation handling


PROJECT=$1
COLLECTION=$2
STAGE_LOCATION=$3
TMP_LOCATION=$4
TEXT_PIPELINE_KEY=$5
TEXT_PIPELINE_VERSION=$6
SENTENCE_PIPELINE_KEY=$7
SENTENCE_PIPELINE_VERSION=$8
ABBREVIATION_PIPELINE_VERSION=$9
OUTPUT_PIPELINE_VERSION=${10}
OPTIONAL_DOCUMENT_SPECIFIC_COLLECTION=${11}
JAR_VERSION=${12}
OVERWRITE=${13}

JOB_NAME=$(echo "DOC-TEXT-AUG-${COLLECTION}-${TEXT_PIPELINE_KEY}" | tr '_' '-')

echo "COLLECTION: $COLLECTION"
echo "PROJECT: $PROJECT"
echo "JOB_NAME: $JOB_NAME"
echo "TEXT PIPELINE KEY: $TEXT_PIPELINE_KEY"
echo "TEXT PIPELINE VERSION: $TEXT_PIPELINE_VERSION"
echo "SENTENCE PIPELINE VERSION: $SENTENCE_PIPELINE_VERSION"
echo "ABBREVIATION PIPELINE VERSION: $ABBREVIATION_PIPELINE_VERSION"
echo "OUTPUT_PIPELINE_VERSION KEY: $OUTPUT_PIPELINE_VERSION"
echo "OPTIONAL_DOCUMENT_SPECIFIC_COLLECTION: $OPTIONAL_DOCUMENT_SPECIFIC_COLLECTION"
echo "OVERWRITE KEY: $OVERWRITE"

java -Dfile.encoding=UTF-8 -jar target/tm-pipelines-bundled-${JAR_VERSION}.jar DOC_TEXT_AUGMENTATION \
--jobName="$JOB_NAME" \
--textPipelineKey="$TEXT_PIPELINE_KEY" \
--textPipelineVersion="$TEXT_PIPELINE_VERSION" \
--sentencePipelineKey="$SENTENCE_PIPELINE_KEY" \
--sentencePipelineVersion="$SENTENCE_PIPELINE_VERSION" \
--abbreviationPipelineVersion="$ABBREVIATION_PIPELINE_VERSION" \
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
--autoscalingAlgorithm=THROUGHPUT_BASED \
--defaultWorkerLogLevel=INFO \
--runner=DataflowRunner