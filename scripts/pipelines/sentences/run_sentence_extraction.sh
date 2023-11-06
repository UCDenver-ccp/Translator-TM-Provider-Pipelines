#!/bin/sh

PROJECT=$1
COLLECTION=$2
TEXT_PIPELINE_KEY=$3
TEXT_PIPELINE_VERSION=$4
SENTENCE_PIPELINE_KEY=$5
SENTENCE_PIPELINE_VERSION=$6
CONCEPT_POST_PROCESS_VERSION=$7
ASSOCIATION=$8
PREFIX_X=$9
PLACEHOLDER_X=${10}
PREFIX_Y=${11}
PLACEHOLDER_Y=${12}
STAGE_LOCATION=${13}
TMP_LOCATION=${14}
BUCKET=${15}
JAR_VERSION=${16}

# ASSOCIATION="bl_chemical_to_disease_or_phenotypic_feature"

JOB_NAME=$(echo "SENTENCE-EXTRACTION-${ASSOCIATION}-${COLLECTION}" | tr '_' '-')

echo "ASSOCIATION: $ASSOCIATION"
echo "COLLECTION: $COLLECTION"
echo "TEXT_PIPELINE_KEY: $TEXT_PIPELINE_KEY"
echo "TEXT_PIPELINE_VERSION: $TEXT_PIPELINE_VERSION"
echo "SENTENCE_PIPELINE_KEY: $SENTENCE_PIPELINE_KEY"
echo "SENTENCE_PIPELINE_VERSION: $SENTENCE_PIPELINE_VERSION"
echo "CONCEPT_POST_PROCESS_VERSION: $CONCEPT_POST_PROCESS_VERSION"
echo "PREFIX_X: $PREFIX_X"
echo "PLACEHOLDER_X: $PLACEHOLDER_X"
echo "PREFIX_Y: $PREFIX_Y"
echo "PLACEHOLDER_Y: $PLACEHOLDER_Y"
echo "JAR_VERSION: $JAR_VERSION"
echo "PROJECT: $PROJECT"
echo "JOB_NAME: $JOB_NAME"

TSV_OUTPUT_BUCKET="${BUCKET}/output/sentences_tsv/${ASSOCIATION}/${COLLECTION}/${ASSOCIATION}"
DP_INPUT_OUTPUT_BUCKET="${BUCKET}/output/sentences_dp_input/${ASSOCIATION}/${COLLECTION}/${ASSOCIATION}"
# ANCESTOR_MAP_FILE_PATH="${BUCKET}/ontology-resources/ontology-class-ancestor-map.tsv.gz"

java -Dfile.encoding=UTF-8 -jar "target/tm-pipelines-bundled-${JAR_VERSION}.jar" SENTENCE_EXTRACTION \
--jobName="$JOB_NAME" \
--targetProcessingStatusFlag='NOOP' \
--inputDocumentCriteria="TEXT|TEXT|${TEXT_PIPELINE_KEY}|${TEXT_PIPELINE_VERSION};SECTIONS|BIONLP|${TEXT_PIPELINE_KEY}|${TEXT_PIPELINE_VERSION};SENTENCE|BIONLP|${SENTENCE_PIPELINE_KEY}|${SENTENCE_PIPELINE_VERSION};CONCEPT_ALL|BIONLP|CONCEPT_POST_PROCESS|${CONCEPT_POST_PROCESS_VERSION}" \
--keywords='' \
--conceptIdsToExclude='CHEBI:36080' \
--collection="$COLLECTION" \
--overwrite='YES' \
--tsvOutputBucket="${TSV_OUTPUT_BUCKET}" \
--dpInputOutputBucket="${DP_INPUT_OUTPUT_BUCKET}" \
--prefixX="${PREFIX_X}" \
--placeholderX="${PLACEHOLDER_X}" \
--prefixY="${PREFIX_Y}" \
--placeholderY="${PLACEHOLDER_Y}" \
--project="${PROJECT}" \
--stagingLocation="$STAGE_LOCATION" \
--gcpTempLocation="$TMP_LOCATION" \
--workerZone=us-central1-c \
--region=us-central1 \
--numWorkers=10 \
--maxNumWorkers=75 \
--autoscalingAlgorithm=THROUGHPUT_BASED \
--runner=DataflowRunner


# --ancestorMapFilePath="$ANCESTOR_MAP_FILE_PATH" \
# --ancestorMapFileDelimiter='TAB' \
# --ancestorMapFileSetDelimiter='PIPE' \