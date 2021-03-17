#!/bin/sh

PROJECT=$1
COLLECTION=$2
STAGE_LOCATION=$3
TMP_LOCATION=$4
BUCKET=$5
TEXT_PIPELINE_KEY=$6


JOB_NAME=$(echo "SENTENCE-EXTRACTION-${COLLECTION}" | tr '_' '-')


echo "COLLECTION: $COLLECTION"
echo "PROJECT: $PROJECT"
echo "JOB_NAME: $JOB_NAME"

OUTPUT_BUCKET="${BUCKET}/output/sentences/chebi-pr/chebi-pr"

java -Dfile.encoding=UTF-8 -jar target/tm-pipelines-bundled-0.1.0.jar SENTENCE_EXTRACTION \
--jobName="$JOB_NAME" \
--targetProcessingStatusFlag='SENTENCE_DONE' \
--inputDocumentCriteria="TEXT|TEXT|$TEXT_PIPELINE_KEY|0.1.0;SENTENCE|BIONLP|SENTENCE_SEGMENTATION|0.1.0;CONCEPT_ALL|BIONLP|CONCEPT_POST_PROCESS|0.1.0" \
--keywords='' \
--collection="$COLLECTION" \
--overwrite='YES' \
--outputBucket="${OUTPUT_BUCKET}" \
--prefixX='CHEBI' \
--placeholderX='@CHEMICAL$' \
--prefixY='PR' \
--placeholderY='@GENE$' \
--project="${PROJECT}" \
--stagingLocation="$STAGE_LOCATION" \
--gcpTempLocation="$TMP_LOCATION" \
--workerZone=us-central1-c \
--region=us-central1 \
--numWorkers=10 \
--maxNumWorkers=125 \
--runner=DataflowRunner
#--workerMachineType=n1-highmem-2 \