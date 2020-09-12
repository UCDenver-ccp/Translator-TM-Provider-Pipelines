#!/bin/sh

PROJECT=$1
COLLECTION=$2
STAGE_LOCATION=$3
TMP_LOCATION=$4
BUCKET=$5


ZONE='us-central1-c'
JOB_NAME=$(echo "NGD-STORE-COUNTS-${COLLECTION}" | tr '_' '-')
DISK_TYPE='compute.googleapis.com/projects/${PROJECT}/zones/${ZONE}/diskTypes/pd-ssd'
DISK_SIZE_GB=50

echo "COLLECTION: $COLLECTION"
echo "PROJECT: $PROJECT"
echo "JOB_NAME: $JOB_NAME"
echo "DISK_TYPE: $DISK_TYPE"

ANCESTOR_MAP_FILE_PATH='${BUCKET}/ontology-resources/ontology-class-ancestor-map.tsv.gz'
OUTPUT_BUCKET='${BUCKET}/output/ngd-concept-counts'

INPUT_DOC_CRITERIA='TEXT|TEXT|MEDLINE_XML_TO_TEXT|0.1.0;CONCEPT_ALL|BIONLP|CONCEPT_POST_PROCESS|0.1.0'
REQUIRED_FLAGS='CONCEPT_POST_PROCESSING_DONE'


java -Dfile.encoding=UTF-8 -jar target/tm-pipelines-bundled-0.1.0.jar NORMALIZED_GOOGLE_DISTANCE_CONCEPT_STORE_COUNTS \
--jobName=$JOB_NAME \
--inputDocumentCriteria=$INPUT_DOC_CRITERIA \
--requiredProcessingStatusFlags=$REQUIRED_FLAGS \
--ancestorMapFilePath=$ANCESTOR_MAP_FILE_PATH \
--ancestorMapFileDelimiter='TAB' \
--ancestorMapFileSetDelimiter='PIPE' \
--cooccurLevel='DOCUMENT' \
--collection=$COLLECTION \
--overwrite='YES' \
--outputBucket=$OUTPUT_BUCKET \
--project=${PROJECT} \
--stagingLocation=$STAGE_LOCATION \
--gcpTempLocation=$TMP_LOCATION \
--zone=$ZONE \
--numWorkers=10 \
--maxNumWorkers=125 \
--runner=DataflowRunner
#--diskSizeGb=$DISK_SIZE_GB \
#--workerDiskType=$DISK_TYPE \
