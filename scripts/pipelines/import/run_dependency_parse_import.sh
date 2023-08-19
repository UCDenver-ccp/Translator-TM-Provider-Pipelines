#!/bin/sh

PROJECT=$1
COLLECTION=$2
BASE_DP_FILE_PATH=$3
OVERWRITE_FLAG=$4
STAGE_LOCATION=$5
TMP_LOCATION=$6
JAR_VERSION=$7

ZONE='us-central1-c'
REGION='us-central1'
JOB_NAME=$(echo "DEPENDENCY-PARSE-IMPORT-${COLLECTION}" | tr '_' '-')


echo "COLLECTION: $COLLECTION"
echo "PROJECT: $PROJECT"
echo "BASE_DP_FILE_PATH: $BASE_DP_FILE_PATH"
echo "OVERWRITE_FLAG: $OVERWRITE_FLAG"
echo "JOB_NAME: $JOB_NAME"

java -Dfile.encoding=UTF-8 -jar "target/tm-pipelines-bundled-${JAR_VERSION}.jar" DEPENDENCY_PARSE_IMPORT \
--jobName="$JOB_NAME" \
--baseDependencyParseFilePath="$BASE_DP_FILE_PATH" \
--collection="$COLLECTION" \
--overwrite="$OVERWRITE_FLAG" \
--project="${PROJECT}" \
--stagingLocation="$STAGE_LOCATION" \
--gcpTempLocation="$TMP_LOCATION" \
--workerZone="$ZONE" \
--region="$REGION" \
--numWorkers=10 \
--maxNumWorkers=200 \
--autoscalingAlgorithm=THROUGHPUT_BASED \
--runner=DataflowRunner