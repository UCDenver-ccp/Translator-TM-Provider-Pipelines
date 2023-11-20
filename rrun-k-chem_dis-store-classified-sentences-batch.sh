#!/bin/sh

source ./rrun.env.sh

SCRIPT=./scripts/pipelines/sentences/run_classified_sentence_storage.sh

# TEXT_PIPELINE_KEY=MEDLINE_XML_TO_TEXT
# TEXT_PIPELINE_VERSION="recent"
# MAX_SUBSET_INDEX=38
# SUBSET_PREFIX=PUBMED_SUB_

# SENTENCE_PIPELINE_KEY=SENTENCE_SEGMENTATION
# SENTENCE_PIPELINE_VERSION="recent"
# CONCEPT_POST_PROCESS_VERSION="0.3.0"


# For chemical/drug to disease/phenotype sentences
ASSOCIATION="bl_chemical_to_disease_or_phenotypic_feature"
BERT_MODEL_VERSION="0.4"
OUTPUT_VERSION="0.2.0"

# halt if the MYSQL_PASSWORD environment variable is not set
if [ -z "$DB_PASSWORD" ]
then
      echo "\$var is empty!!!!!!!"
      exit 1
fi





# ---------------------------------------------
# process a single collection
# COLLECTION="PUBMED_SUB_37"
# $SCRIPT $PROJECT_ID ${COLLECTION} $TEXT_PIPELINE_KEY $TEXT_PIPELINE_VERSION $SENTENCE_PIPELINE_KEY $SENTENCE_PIPELINE_VERSION $CONCEPT_POST_PROCESS_VERSION $ASSOCIATION $PREFIX_X $PLACEHOLDER_X $PREFIX_Y $PLACEHOLDER_Y ${STAGE_LOCATION} ${TEMP_LOCATION} ${WORK_BUCKET} $JAR_VERSION ${OUTPUT_VERSION} &> "./logs/sentence-extraction-${COLLECTION}-${ASSOCIATION}.log" &

OUTPUT_VERSION="0.2.0" # v0.2.0 is a run after large concept recognition error analysis and updates

# ---------------------------------------------
# use the below for bulk processing
#
for INDEX in $(seq 0 4 $MAX_SUBSET_INDEX)  
  do 
    ind=$(($INDEX + 0))
    if (( ind <= $MAX_SUBSET_INDEX)); then
        echo "Starting sentence storage pipeline... ${ind} $(date)"
        COLLECTION="${SUBSET_PREFIX}${ind}"
        OUTPUT_COLLECTION=$COLLECTION
        $SCRIPT $PROJECT_ID ${COLLECTION} ${STAGE_LOCATION} ${TEMP_LOCATION} ${WORK_BUCKET} ${BIOLINK_ASSOCIATION} ${BERT_MODEL_VERSION} ${DATABASE_NAME} ${DB_USER_NAME} ${DB_PASSWORD} ${CLOUD_SQL_REGION} ${MYSQL_INSTANCE_NAME} &> ./logs/classified-sentence-storage-${NUM_G}.log &
        sleep 120
    fi
    ind=$(($INDEX + 1))
    if (( ind <= $MAX_SUBSET_INDEX)); then
        echo "Starting sentence extraction pipeline... ${ind} $(date)"
        COLLECTION="${SUBSET_PREFIX}${ind}"
        OUTPUT_COLLECTION=$COLLECTION
        $SCRIPT $PROJECT_ID ${COLLECTION} $TEXT_PIPELINE_KEY $TEXT_PIPELINE_VERSION $SENTENCE_PIPELINE_KEY $SENTENCE_PIPELINE_VERSION $CONCEPT_POST_PROCESS_VERSION $ASSOCIATION $PREFIX_X $PLACEHOLDER_X $PREFIX_Y $PLACEHOLDER_Y ${STAGE_LOCATION} ${TEMP_LOCATION} ${WORK_BUCKET} $JAR_VERSION ${OUTPUT_VERSION} ${OUTPUT_COLLECTION} &> "./logs/sentence-extraction-${COLLECTION}-${ASSOCIATION}-${OUTPUT_VERSION}.log" &
        sleep 120
    fi
    ind=$(($INDEX + 2))
    if (( ind <= $MAX_SUBSET_INDEX)); then
        echo "Starting sentence extraction pipeline... ${ind} $(date)"
        COLLECTION="${SUBSET_PREFIX}${ind}"
        OUTPUT_COLLECTION=$COLLECTION
        $SCRIPT $PROJECT_ID ${COLLECTION} $TEXT_PIPELINE_KEY $TEXT_PIPELINE_VERSION $SENTENCE_PIPELINE_KEY $SENTENCE_PIPELINE_VERSION $CONCEPT_POST_PROCESS_VERSION $ASSOCIATION $PREFIX_X $PLACEHOLDER_X $PREFIX_Y $PLACEHOLDER_Y ${STAGE_LOCATION} ${TEMP_LOCATION} ${WORK_BUCKET} $JAR_VERSION ${OUTPUT_VERSION} ${OUTPUT_COLLECTION} &> "./logs/sentence-extraction-${COLLECTION}-${ASSOCIATION}-${OUTPUT_VERSION}.log" &
        sleep 120
    fi
    ind=$(($INDEX + 3))
    if (( ind <= $MAX_SUBSET_INDEX)); then
        echo "Starting sentence extraction pipeline... ${ind} $(date)"
        COLLECTION="${SUBSET_PREFIX}${ind}"
        OUTPUT_COLLECTION=$COLLECTION
        $SCRIPT $PROJECT_ID ${COLLECTION} $TEXT_PIPELINE_KEY $TEXT_PIPELINE_VERSION $SENTENCE_PIPELINE_KEY $SENTENCE_PIPELINE_VERSION $CONCEPT_POST_PROCESS_VERSION $ASSOCIATION $PREFIX_X $PLACEHOLDER_X $PREFIX_Y $PLACEHOLDER_Y ${STAGE_LOCATION} ${TEMP_LOCATION} ${WORK_BUCKET} $JAR_VERSION ${OUTPUT_VERSION} ${OUTPUT_COLLECTION} &> "./logs/sentence-extraction-${COLLECTION}-${ASSOCIATION}-${OUTPUT_VERSION}.log" &
    fi
    wait 
  done
