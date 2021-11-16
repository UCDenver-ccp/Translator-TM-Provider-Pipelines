#!/bin/sh

PROJECT=$1
STAGE_LOCATION=$2
TMP_LOCATION=$3
BUCKET=$4
DATABASE_NAME=$5
DB_USER_NAME=$6
DB_PASSWORD=$7
MYSQL_INSTANCE_NAME=$8
CLOUD_SQL_REGION=${9}

JOB_NAME='CONCEPT-IDF'
COUNT_FILE_BUCKET="${BUCKET}/output/concept-cooccurrence-counts"
ANCESTOR_MAP_FILE_PATH="${BUCKET}/ontology-resources/ontology-class-ancestor-map.tsv.gz"

echo "PROJECT: $PROJECT"
echo "JOB_NAME: $JOB_NAME"

java -Dfile.encoding=UTF-8 -jar target/tm-pipelines-bundled-0.1.0.jar CONCEPT_IDF \
--jobName="$JOB_NAME" \
--countFileBucket="$COUNT_FILE_BUCKET" \
--ancestorMapFilePath="$ANCESTOR_MAP_FILE_PATH" \
--ancestorMapFileDelimiter='TAB' \
--ancestorMapFileSetDelimiter='PIPE' \
--databaseName="$DATABASE_NAME" \
--dbUsername="$DB_USER_NAME" \
--dbPassword="$DB_PASSWORD" \
--mySqlInstanceName="$MYSQL_INSTANCE_NAME" \
--cloudSqlRegion="$CLOUD_SQL_REGION" \
--project="${PROJECT}" \
--stagingLocation="$STAGE_LOCATION" \
--gcpTempLocation="$TMP_LOCATION" \
--workerZone=us-central1-c \
--region=us-central1 \
--numWorkers=10 \
--maxNumWorkers=200 \
--autoscalingAlgorithm=THROUGHPUT_BASED \
--runner=DataflowRunner