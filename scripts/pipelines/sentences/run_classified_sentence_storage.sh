#!/bin/sh

PROJECT=$1
COLLECTION=$2
STAGE_LOCATION=$3
TMP_LOCATION=$4
BUCKET=$5
BIOLINK_ASSOCIATION=$6
BERT_MODEL_VERSION=$7
DATABASE_NAME=$8
DB_USER_NAME=$9
DB_PASSWORD=${10}
CLOUD_SQL_REGION=${11}
MYSQL_INSTANCE_NAME=${12}


ZONE='us-central1-c'
REGION='us-central1'
JOB_NAME=$(echo "CLASSIFIED-SENTENCE-STORAGE-${COLLECTION}" | tr '_' '-')

ASSOCIATION_KEY_LC=$(echo "$BIOLINK_ASSOCIATION" | tr '[:upper:]' '[:lower:]')

BERT_INPUT_FILE_NAME_WITH_METADATA="bert-input-${ASSOCIATION_KEY_LC}.metadata.${COLLECTION}.tsv"
TO_BE_CLASSIFIED_SENTENCE_BUCKET="${BUCKET}/output/sentences/${ASSOCIATION_KEY_LC}/${COLLECTION}"
CLASSIFIED_SENTENCE_PATH="${BUCKET}/output/classified_sentences/${ASSOCIATION_KEY_LC}/${BERT_MODEL_VERSION}"
CLASSIFIED_SENTENCE_FILE_NAME="${ASSOCIATION_KEY_LC}.${BERT_MODEL_VERSION}.${COLLECTION}.classified_sentences.tsv.gz"
CLASSIFIED_SENTENCE_FILE="${CLASSIFIED_SENTENCE_PATH}/${CLASSIFIED_SENTENCE_FILE_NAME}"
METADATA_SENTENCE_FILE="${TO_BE_CLASSIFIED_SENTENCE_BUCKET}/${BERT_INPUT_FILE_NAME_WITH_METADATA}"

echo "COLLECTION: $COLLECTION"
echo "PROJECT: $PROJECT"
echo "JOB_NAME: $JOB_NAME"
echo "BERT_INPUT_FILE_NAME_WITH_METADATA: $BERT_INPUT_FILE_NAME_WITH_METADATA"
echo "TO_BE_CLASSIFIED_SENTENCE_BUCKET: $TO_BE_CLASSIFIED_SENTENCE_BUCKET"
echo "CLASSIFIED_SENTENCE_PATH: $CLASSIFIED_SENTENCE_PATH"
echo "CLASSIFIED_SENTENCE_FILE_NAME: $CLASSIFIED_SENTENCE_FILE_NAME"
echo "CLASSIFIED_SENTENCE_FILE: $CLASSIFIED_SENTENCE_FILE"
echo "METADATA_SENTENCE_FILE: $METADATA_SENTENCE_FILE"
echo "ASSOCIATION_KEY_LC: $ASSOCIATION_KEY_LC"
echo "BIOLINK_ASSOCIATION: $BIOLINK_ASSOCIATION"
echo "BERT_MODEL_VERSION: $BERT_MODEL_VERSION"
echo "DATABASE_NAME: $DATABASE_NAME"
echo "DB_USER_NAME: $DB_USER_NAME"
echo "CLOUD_SQL_REGION: $CLOUD_SQL_REGION"
echo "MYSQL_INSTANCE_NAME: $MYSQL_INSTANCE_NAME"

java -Dfile.encoding=UTF-8 -jar target/tm-pipelines-bundled-0.1.0.jar CLASSIFIED_SENTENCE_STORAGE \
--jobName="$JOB_NAME" \
--bertOutputFilePath="$CLASSIFIED_SENTENCE_FILE" \
--sentenceMetadataFilePath="$METADATA_SENTENCE_FILE" \
--biolinkAssociation="$BIOLINK_ASSOCIATION" \
--databaseName="$DATABASE_NAME" \
--dbUsername="$DB_USER_NAME" \
--dbPassword="$DB_PASSWORD" \
--mySqlInstanceName="$MYSQL_INSTANCE_NAME" \
--cloudSqlRegion="$CLOUD_SQL_REGION" \
--bertScoreInclusionMinimumThreshold=0.9 \
--project="${PROJECT}" \
--stagingLocation="$STAGE_LOCATION" \
--gcpTempLocation="$TMP_LOCATION" \
--workerZone="$ZONE" \
--region="$REGION" \
--numWorkers=10 \
--maxNumWorkers=50 \
--autoscalingAlgorithm=THROUGHPUT_BASED \
--runner=DataflowRunner
