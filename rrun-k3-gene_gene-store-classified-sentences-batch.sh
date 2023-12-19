#!/bin/sh

source ./rrun.env.sh

SCRIPT=./scripts/pipelines/sentences/run_classified_sentence_storage.sh

# MAX_SUBSET_INDEX=38
# SUBSET_PREFIX=PUBMED_SUB_

MAX_SUBSET_INDEX=41
SUBSET_PREFIX=PMC_SUBSET_


# For bl_gene_regulatory_relationship sentences
BIOLINK_ASSOCIATION="BL_GENE_REGULATORY_RELATIONSHIP"
BERT_MODEL_VERSION="0.1"

# halt if the MYSQL_PASSWORD environment variable is not set
if [ -z "$DB_PASSWORD" ]
then
      echo "\$var is empty!!!!!!!"
      exit 1
fi

# ---------------------------------------------
# process a single collection
# COLLECTION="PMC_SUBSET_0"
# $SCRIPT $PROJECT_ID ${COLLECTION} ${STAGE_LOCATION} ${TEMP_LOCATION} ${WORK_BUCKET} ${BIOLINK_ASSOCIATION} ${BERT_MODEL_VERSION} ${DATABASE_NAME} ${DB_USER_NAME} ${DB_PASSWORD} ${CLOUD_SQL_REGION} ${MYSQL_INSTANCE_NAME} ${SENTENCE_VERSION} ${DATABASE_VERSION} ${JAR_VERSION} &> "./logs/classified-sentence-storage-${COLLECTION}-${BIOLINK_ASSOCIATION}-${SENTENCE_VERSION}-${BERT_MODEL_VERSION}-${DATABASE_VERSION}.log" &


# ---------------------------------------------
# use the below for bulk processing
#
for INDEX in $(seq 1 1 $MAX_SUBSET_INDEX)  
  do 
    ind=$(($INDEX + 0))
    if (( ind <= $MAX_SUBSET_INDEX)); then
        echo "Starting sentence storage pipeline... ${ind} $(date)"
        COLLECTION="${SUBSET_PREFIX}${ind}"
        OUTPUT_COLLECTION=$COLLECTION
        $SCRIPT $PROJECT_ID ${COLLECTION} ${STAGE_LOCATION} ${TEMP_LOCATION} ${WORK_BUCKET} ${BIOLINK_ASSOCIATION} ${BERT_MODEL_VERSION} ${DATABASE_NAME} ${DB_USER_NAME} ${DB_PASSWORD} ${CLOUD_SQL_REGION} ${MYSQL_INSTANCE_NAME} ${SENTENCE_VERSION} ${DATABASE_VERSION} ${JAR_VERSION} &> "./logs/classified-sentence-storage-${COLLECTION}-${BIOLINK_ASSOCIATION}-${SENTENCE_VERSION}-${BERT_MODEL_VERSION}-${DATABASE_VERSION}.log" &
    fi
    wait 
  done
