#!/bin/sh

PROJECT=$1
STAGE_LOCATION=$2
TMP_LOCATION=$3
BUCKET=$4
COOCCUR_LEVELS_TO_PROCESS=$5
DATABASE_NAME=$6
DB_USER_NAME=$7
DB_PASSWORD=$8
MYSQL_INSTANCE_NAME=$9
CLOUD_SQL_REGION=${10}

JOB_NAME='CONCEPT-METRICS'
COUNT_FILE_BUCKET="${BUCKET}/output/concept-cooccurrence-counts"
ANCESTOR_MAP_FILE_PATH="${BUCKET}/ontology-resources/ontology-class-ancestor-map.tsv.gz"

echo "PROJECT: $PROJECT"
echo "JOB_NAME: $JOB_NAME"

java -Dfile.encoding=UTF-8 -jar target/tm-pipelines-bundled-0.1.0.jar CONCEPT_COOCCURRENCE_METRICS \
--jobName="$JOB_NAME" \
--cooccurLevelsToProcess="$COOCCUR_LEVELS_TO_PROCESS" \
--countFileBucket="$COUNT_FILE_BUCKET" \
--addAncestors=false \
--ancestorMapFilePath="$ANCESTOR_MAP_FILE_PATH" \
--ancestorMapFileDelimiter='TAB' \
--ancestorMapFileSetDelimiter='PIPE' \
--databaseName="$DATABASE_NAME" \
--dbUsername="$DB_USER_NAME" \
--dbPassword="$DB_PASSWORD" \
--mySqlInstanceName="$MYSQL_INSTANCE_NAME" \
--cloudSqlRegion="$CLOUD_SQL_REGION" \
--conceptPrefixesToInclude="" \
--project="${PROJECT}" \
--stagingLocation="$STAGE_LOCATION" \
--gcpTempLocation="$TMP_LOCATION" \
--workerZone=us-central1-c \
--region=us-central1 \
--numWorkers=10 \
--maxNumWorkers=200 \
--autoscalingAlgorithm=THROUGHPUT_BASED \
--runner=DataflowRunner