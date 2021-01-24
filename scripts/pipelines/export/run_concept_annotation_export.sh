#!/bin/sh

PROJECT=$1
COLLECTION=$2
TEXT_PIPELINE_KEY=$3
OUTPUT_BUCKET=$4
STAGE_LOCATION=$5
TMP_LOCATION=$6

JOB_NAME=$(echo "CONCEPT-ANNOT-EXPORT-${COLLECTION}" | tr '_' '-')


echo "COLLECTION: $COLLECTION"
echo "PROJECT: $PROJECT"
echo "TEXT_PIPELINE_KEY: $TEXT_PIPELINE_KEY"
echo "OUTPUT_BUCKET: $OUTPUT_BUCKET"
echo "JOB_NAME: $JOB_NAME"

java -Dfile.encoding=UTF-8 -jar target/tm-pipelines-bundled-0.1.0.jar CONCEPT_ANNOTATION_EXPORT \
--jobName="$JOB_NAME" \
--textInputPipelineKey="$TEXT_PIPELINE_KEY" \
--textInputPipelineVersion="0.1.0" \
--conceptPostProcessPipelineVersion="0.1.0" \
--collection="$COLLECTION" \
--outputBucket="$OUTPUT_BUCKET" \
--project="${PROJECT}" \
--stagingLocation="$STAGE_LOCATION" \
--gcpTempLocation="$TMP_LOCATION" \
--zone=us-central1-c \
--numWorkers=10 \
--maxNumWorkers=200 \
--runner=DataflowRunner