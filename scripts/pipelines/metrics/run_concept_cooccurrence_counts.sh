#!/bin/sh

PROJECT=$1
COLLECTION=$2
STAGE_LOCATION=$3
TMP_LOCATION=$4
BUCKET=$5


ZONE='us-central1-c'
REGION='us-central1'
JOB_NAME=$(echo "CONCEPT-COOCCURRENCE-COUNTS-${COLLECTION}" | tr '_' '-')

echo "COLLECTION: $COLLECTION"
echo "PROJECT: $PROJECT"
echo "JOB_NAME: $JOB_NAME"

ANCESTOR_MAP_FILE_PATH="${BUCKET}/ontology-resources/ontology-class-ancestor-map.tsv.gz"

Default settings below
OUTPUT_BUCKET="${BUCKET}/output/concept-cooccurrence-counts"
INPUT_DOC_CRITERIA='TEXT|TEXT|MEDLINE_XML_TO_TEXT|0.1.0;SECTIONS|BIONLP|MEDLINE_XML_TO_TEXT|0.1.0;CONCEPT_ALL|BIONLP|CONCEPT_POST_PROCESS|0.1.0;SENTENCE|BIONLP|SENTENCE_SEGMENTATION|0.1.0'
REQUIRED_FLAGS='CONCEPT_POST_PROCESSING_DONE'

java -Dfile.encoding=UTF-8 -jar target/tm-pipelines-bundled-0.1.0.jar CONCEPT_COOCCURRENCE_COUNTS \
--jobName="$JOB_NAME" \
--inputDocumentCriteria="$INPUT_DOC_CRITERIA" \
--requiredProcessingStatusFlags="$REQUIRED_FLAGS" \
--ancestorMapFilePath="$ANCESTOR_MAP_FILE_PATH" \
--ancestorMapFileDelimiter='TAB' \
--ancestorMapFileSetDelimiter='PIPE' \
--cooccurLevels='ABSTRACT|DOCUMENT|SENTENCE|TITLE' \
--addSuperClassAnnots="YES" \
--docTypeToCount="CONCEPT_ALL" \
--countType="FULL" \
--collection="$COLLECTION" \
--overwrite='YES' \
--outputBucket="$OUTPUT_BUCKET" \
--project="${PROJECT}" \
--stagingLocation="$STAGE_LOCATION" \
--gcpTempLocation="$TMP_LOCATION" \
--workerZone="$ZONE" \
--region="$REGION" \
--numWorkers=10 \
--maxNumWorkers=125 \
--runner=DataflowRunner



# # NOTE: this is set up to counting only concepts from MP ontology
# OUTPUT_BUCKET="${BUCKET}/output/mp-concept-counts"
# INPUT_DOC_CRITERIA='TEXT|TEXT|MEDLINE_XML_TO_TEXT|0.1.0;CONCEPT_MP|BIONLP|OGER|0.1.0'
# REQUIRED_FLAGS='OGER_MP_DONE'

# java -Dfile.encoding=UTF-8 -jar target/tm-pipelines-bundled-0.1.0.jar CONCEPT_COOCCURRENCE_COUNTS \
# --jobName="$JOB_NAME" \
# --inputDocumentCriteria="$INPUT_DOC_CRITERIA" \
# --requiredProcessingStatusFlags="$REQUIRED_FLAGS" \
# --ancestorMapFilePath="$ANCESTOR_MAP_FILE_PATH" \
# --ancestorMapFileDelimiter='TAB' \
# --ancestorMapFileSetDelimiter='PIPE' \
# --cooccurLevel='DOCUMENT' \
# --addSuperClassAnnots="NO" \
# --docTypeToCount="CONCEPT_MP" \
# --countType="SIMPLE" \
# --collection="$COLLECTION" \
# --overwrite='YES' \
# --outputBucket="$OUTPUT_BUCKET" \
# --project="${PROJECT}" \
# --stagingLocation="$STAGE_LOCATION" \
# --gcpTempLocation="$TMP_LOCATION" \
# --workerZone="$ZONE" \
# --region="$REGION" \
# --numWorkers=10 \
# --maxNumWorkers=100 \
# --runner=DataflowRunner

