#!/bin/sh

PROJECT=$1
STAGE_LOCATION=$2
TMP_LOCATION=$3
BUCKET=$4

JOB_NAME='NGD-KGX'


SINGLETON_FILE_PATTERN="${BUCKET}/output/ngd-concept-counts/concept-to-doc.*"
PAIR_FILE_PATTERN="${BUCKET}/output/ngd-concept-counts/concept-pair-to-doc.*"
CONCEPT_COUNT_FILE_PATTERN="${BUCKET}/output/ngd-concept-counts/doc-to-concept-count.*"
LABEL_MAP_FILE="${BUCKET}/ontology-resources/ontology-class-label-map.tsv.gz"
CATEGORY_MAP_FILE="${BUCKET}/ontology-resources/ontology-class-biolink-category-map.tsv.gz"
OUTPUT_BUCKET="${BUCKET}/output/ngd-concept-kgx/ngd-cooccur-kgx-"


echo "COLLECTION: $COLLECTION"
echo "PROJECT: $PROJECT"
echo "JOB_NAME: $JOB_NAME"

java -Dfile.encoding=UTF-8 -jar target/tm-pipelines-bundled-0.1.0.jar CONCEPT_COOCCURRENCE_METRICS \
--jobName="$JOB_NAME" \
--singletonFilePattern="$SINGLETON_FILE_PATTERN" \
--pairFilePattern="$PAIR_FILE_PATTERN" \
--conceptCountFilePattern="$CONCEPT_COUNT_FILE_PATTERN" \
--labelMapFilePattern="$LABEL_MAP_FILE" \
--labelMapFileDelimiter='TAB' \
--categoryMapFilePattern="$CATEGORY_MAP_FILE" \
--categoryMapFileDelimiter='TAB' \
--outputBucket="$OUTPUT_BUCKET" \
--project="${PROJECT}" \
--stagingLocation="$STAGE_LOCATION" \
--gcpTempLocation="$TMP_LOCATION" \
--zone=us-central1-c \
--numWorkers=10 \
--maxNumWorkers=200 \
--runner=DataflowRunner