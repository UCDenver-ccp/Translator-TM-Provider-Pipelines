#!/bin/sh

PROJECT=$1
COLLECTION=$2
STAGE_LOCATION=$3
TMP_LOCATION=$4
BUCKET=$5


JOB_NAME=$(echo "SENTENCE-EXTRACTION-MONDO-PR-${COLLECTION}" | tr '_' '-')


echo "COLLECTION: $COLLECTION"
echo "PROJECT: $PROJECT"
echo "JOB_NAME: $JOB_NAME"

OUTPUT_BUCKET="${BUCKET}/extracted-sentences/mondo-pr/mondo-pr"

java -Dfile.encoding=UTF-8 -jar target/tm-pipelines-bundled-0.1.0.jar SENTENCE_EXTRACTION \
--jobName="$JOB_NAME" \
--targetProcessingStatusFlag='SENTENCE_DONE' \
--inputDocumentCriteria='TEXT|TEXT|MEDLINE_XML_TO_TEXT|0.1.0;SENTENCE|BIONLP|SENTENCE_SEGMENTATION|0.1.0;CONCEPT_ALL|BIONLP|CONCEPT_POST_PROCESS|0.1.0' \
--keywords='' \
--collection="$COLLECTION" \
--overwrite='YES' \
--outputBucket="${OUTPUT_BUCKET}" \
--prefixX='PR' \
--placeholderX='@PROTEIN$' \
--prefixY='MONDO' \
--placeholderY='@DISEASE$' \
--project="${PROJECT}" \
--stagingLocation="$STAGE_LOCATION" \
--gcpTempLocation="$TMP_LOCATION" \
--zone=us-central1-c \
--numWorkers=10 \
--maxNumWorkers=125 \
--runner=DataflowRunner
#--workerMachineType=n1-highmem-2 \