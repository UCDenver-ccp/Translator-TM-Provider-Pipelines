#!/bin/sh

source ./rrun.env.sh

#### PUBMED
TEXT_PIPELINE_KEY=MEDLINE_XML_TO_TEXT
TEXT_PIPELINE_VERSION="recent"
SUBSET_PREFIX=PUBMED_SUB_
MAX_SUBSET_INDEX=36

#### PMCOA
# TEXT_PIPELINE_KEY=BIOC_TO_TEXT
# TEXT_PIPELINE_VERSION="0.1.0"
# SUBSET_PREFIX=PMC_SUBSET_
# MAX_SUBSET_INDEX=37

SCRIPT=./scripts/pipelines/sentences/run_sentence_segment.sh

JAR_VERSION="0.2.1"

OVERWRITE=YES

# # use the below to run a single collection
# COLLECTION="PUBMED_SUB_37"
# echo "Starting SENTENCE SEGMENTATION pipeline... ${COLLECTION}"
# $SCRIPT $PROJECT_ID ${COLLECTION} ${STAGE_LOCATION} ${TEMP_LOCATION} $TEXT_PIPELINE_KEY $TEXT_PIPELINE_VERSION $OVERWRITE $JAR_VERSION &> "./logs/sentence-${COLLECTION}.log" &

for INDEX in $(seq 0 4 $MAX_SUBSET_INDEX)  
do 
ind=$(($INDEX + 0))
if (( ind <= $MAX_SUBSET_INDEX)); then
    echo "Starting SENTENCE SEGMENTATION pipeline... ${ind} $(date)"
    $SCRIPT $PROJECT_ID ${SUBSET_PREFIX}${ind} ${STAGE_LOCATION} ${TEMP_LOCATION} $TEXT_PIPELINE_KEY $TEXT_PIPELINE_VERSION $OVERWRITE $JAR_VERSION &> "./logs/sentence-${SUBSET_PREFIX}${ind}.log" &
    sleep 120
fi
ind=$(($INDEX + 1))
if (( ind <= $MAX_SUBSET_INDEX)); then
    echo "Starting SENTENCE SEGMENTATION pipeline... ${ind} $(date)"
    $SCRIPT $PROJECT_ID ${SUBSET_PREFIX}${ind} ${STAGE_LOCATION} ${TEMP_LOCATION} $TEXT_PIPELINE_KEY $TEXT_PIPELINE_VERSION $OVERWRITE $JAR_VERSION &> "./logs/sentence-${SUBSET_PREFIX}${ind}.log" &
    sleep 120
fi
ind=$(($INDEX + 2))
if (( ind <= $MAX_SUBSET_INDEX)); then
    echo "Starting SENTENCE SEGMENTATION pipeline... ${ind} $(date)"
    $SCRIPT $PROJECT_ID ${SUBSET_PREFIX}${ind} ${STAGE_LOCATION} ${TEMP_LOCATION} $TEXT_PIPELINE_KEY $TEXT_PIPELINE_VERSION $OVERWRITE $JAR_VERSION &> "./logs/sentence-${SUBSET_PREFIX}${ind}.log" &
    sleep 120
fi
ind=$(($INDEX + 3))
if (( ind <= $MAX_SUBSET_INDEX)); then
    echo "Starting SENTENCE SEGMENTATION pipeline... ${ind} $(date)"
    $SCRIPT $PROJECT_ID ${SUBSET_PREFIX}${ind} ${STAGE_LOCATION} ${TEMP_LOCATION} $TEXT_PIPELINE_KEY $TEXT_PIPELINE_VERSION $OVERWRITE $JAR_VERSION &> "./logs/sentence-${SUBSET_PREFIX}${ind}.log" &
fi
wait 
done


