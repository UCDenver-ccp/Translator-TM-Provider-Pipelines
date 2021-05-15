#!/bin/sh

PROJECT=$1
COLLECTION=$2
STAGE_LOCATION=$3
TMP_LOCATION=$4
BUCKET=$5


#ZONE='us-central1-c'
JOB_NAME=$(echo "SIMPLE-CONCEPT-COUNTS-${COLLECTION}" | tr '_' '-')
#DISK_TYPE="compute.googleapis.com/projects/${PROJECT}/zones/${ZONE}/diskTypes/pd-ssd"
#DISK_SIZE_GB=50

echo "COLLECTION: $COLLECTION"
echo "PROJECT: $PROJECT"
echo "JOB_NAME: $JOB_NAME"
echo "DISK_TYPE: $DISK_TYPE"

ANCESTOR_MAP_FILE_PATH="${BUCKET}/ontology-resources/ontology-class-ancestor-map.tsv.gz"
OUTPUT_BUCKET="${BUCKET}/output/concept-counts"

FILTER_FLAG="_UNFILTERED"
#FILTER_FLAG=""

INPUT_DOC_CRITERIA="TEXT|TEXT|MEDLINE_XML_TO_TEXT|0.1.0;CONCEPT_ALL${FILTER_FLAG}|BIONLP|CONCEPT_POST_PROCESS|0.1.0"
REQUIRED_FLAGS='CONCEPT_POST_PROCESSING_UNFILTERED_DONE'


java -Dfile.encoding=UTF-8 -jar target/tm-pipelines-bundled-0.1.0.jar NORMALIZED_GOOGLE_DISTANCE_CONCEPT_STORE_COUNTS \
--jobName="$JOB_NAME" \
--inputDocumentCriteria="$INPUT_DOC_CRITERIA" \
--requiredProcessingStatusFlags="$REQUIRED_FLAGS" \
--ancestorMapFilePath="$ANCESTOR_MAP_FILE_PATH" \
--ancestorMapFileDelimiter='TAB' \
--ancestorMapFileSetDelimiter='PIPE' \
--cooccurLevel='DOCUMENT' \
--addSuperClassAnnots="NO" \
--filterFlag="NONE" \
--countType="SIMPLE" \
--collection="$COLLECTION" \
--overwrite='YES' \
--outputBucket="$OUTPUT_BUCKET" \
--project="${PROJECT}" \
--stagingLocation="$STAGE_LOCATION" \
--gcpTempLocation="$TMP_LOCATION" \
--workerZone=us-central1-c \
--region=us-central1 \
--numWorkers=10 \
--maxNumWorkers=100 \
--runner=DataflowRunner

