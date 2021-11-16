#!/bin/sh

PROJECT=$1
COLLECTION=$2
TEXT_PIPELINE_KEY=$3
STAGE_LOCATION=$4
TMP_LOCATION=$5
BUCKET=$6

ASSOCIATION="bl_gene_to_expression_site"

JOB_NAME=$(echo "SENTENCE-EXTRACTION-${ASSOCIATION}-${COLLECTION}" | tr '_' '-')

echo "ASSOCIATION: $ASSOCIATION"
echo "COLLECTION: $COLLECTION"
echo "PROJECT: $PROJECT"
echo "JOB_NAME: $JOB_NAME"

OUTPUT_BUCKET="${BUCKET}/output/sentences/${ASSOCIATION}/${COLLECTION}/${ASSOCIATION}"
ANCESTOR_MAP_FILE_PATH="${BUCKET}/ontology-resources/ontology-class-ancestor-map.tsv.gz"

java -Dfile.encoding=UTF-8 -jar target/tm-pipelines-bundled-0.1.0.jar SENTENCE_EXTRACTION \
--jobName="$JOB_NAME" \
--targetProcessingStatusFlag='NOOP' \
--inputDocumentCriteria="TEXT|TEXT|${TEXT_PIPELINE_KEY}|0.1.0;SECTIONS|BIONLP|${TEXT_PIPELINE_KEY}|0.1.0;SENTENCE|BIONLP|SENTENCE_SEGMENTATION|0.1.0;CONCEPT_ALL|BIONLP|CONCEPT_POST_PROCESS|0.1.0" \
--keywords='' \
--conceptIdsToExclude='CL:0000000|PR:000000001' \
--collection="$COLLECTION" \
--overwrite='YES' \
--outputBucket="${OUTPUT_BUCKET}" \
--prefixX='PR' \
--placeholderX='@GENE$' \
--prefixY='UBERON|CL|GO:0005575' \
--placeholderY='@LOCATION$' \
--ancestorMapFilePath="$ANCESTOR_MAP_FILE_PATH" \
--ancestorMapFileDelimiter='TAB' \
--ancestorMapFileSetDelimiter='PIPE' \
--project="${PROJECT}" \
--stagingLocation="$STAGE_LOCATION" \
--gcpTempLocation="$TMP_LOCATION" \
--workerZone=us-central1-c \
--region=us-central1 \
--numWorkers=10 \
--maxNumWorkers=75 \
--autoscalingAlgorithm=THROUGHPUT_BASED \
--runner=DataflowRunner
