#!/bin/sh

source ./rrun.env.sh

SCRIPT=./scripts/pipelines/sentences/run_sentence_extraction.sh

# TEXT_PIPELINE_KEY=MEDLINE_XML_TO_TEXT
# TEXT_PIPELINE_VERSION="recent"
# SECTION_PIPELINE_KEY=MEDLINE_XML_TO_TEXT
# SECTION_PIPELINE_VERSION="recent"
# MAX_SUBSET_INDEX=38
# SUBSET_PREFIX=PUBMED_SUB_

TEXT_PIPELINE_KEY=FILTER_UNACTIONABLE_TEXT
TEXT_PIPELINE_VERSION="recent"
SECTION_PIPELINE_KEY=BIOC_TO_TEXT
SECTION_PIPELINE_VERSION="recent"
SUBSET_PREFIX=PMC_SUBSET_
MAX_SUBSET_INDEX=41

SENTENCE_PIPELINE_KEY=SENTENCE_SEGMENTATION
SENTENCE_PIPELINE_VERSION="recent"
CONCEPT_POST_PROCESS_VERSION="0.3.0"


# For gene/gene regulatory sentences
ASSOCIATION="bl_gene_regulatory_relationship"
PREFIX_X="PR"
PLACEHOLDER_X="@GENE_REGULATOR$"
PREFIX_Y="PR"
PLACEHOLDER_Y="@REGULATED_GENE$"

OUTPUT_VERSION="0.2.0" # v0.2.0 is a run after large concept recognition error analysis and updates

# # # ---------------------------------------------
# # # process a single collection
# COLLECTION="PMC_SUBSET_0"
# OUTPUT_COLLECTION=$COLLECTION
# $SCRIPT $PROJECT_ID ${COLLECTION} $TEXT_PIPELINE_KEY $TEXT_PIPELINE_VERSION $SECTION_PIPELINE_KEY $SECTION_PIPELINE_VERSION $SENTENCE_PIPELINE_KEY $SENTENCE_PIPELINE_VERSION $CONCEPT_POST_PROCESS_VERSION $ASSOCIATION $PREFIX_X $PLACEHOLDER_X $PREFIX_Y $PLACEHOLDER_Y ${STAGE_LOCATION} ${TEMP_LOCATION} ${WORK_BUCKET} $JAR_VERSION ${OUTPUT_VERSION} ${OUTPUT_COLLECTION} &> "./logs/sentence-extraction-${COLLECTION}-${ASSOCIATION}.log" &

# # ---------------------------------------------
# # use the below for bulk processing
# #
for INDEX in $(seq 1 4 $MAX_SUBSET_INDEX)  
  do 
    ind=$(($INDEX + 0))
    if (( ind <= $MAX_SUBSET_INDEX)); then
        echo "Starting sentence extraction pipeline... ${ind} $(date)"
        COLLECTION="${SUBSET_PREFIX}${ind}"
        OUTPUT_COLLECTION=$COLLECTION
        $SCRIPT $PROJECT_ID ${COLLECTION} $TEXT_PIPELINE_KEY $TEXT_PIPELINE_VERSION $SECTION_PIPELINE_KEY $SECTION_PIPELINE_VERSION $SENTENCE_PIPELINE_KEY $SENTENCE_PIPELINE_VERSION $CONCEPT_POST_PROCESS_VERSION $ASSOCIATION $PREFIX_X $PLACEHOLDER_X $PREFIX_Y $PLACEHOLDER_Y ${STAGE_LOCATION} ${TEMP_LOCATION} ${WORK_BUCKET} $JAR_VERSION ${OUTPUT_VERSION} ${OUTPUT_COLLECTION} &> "./logs/sentence-extraction-${COLLECTION}-${ASSOCIATION}-${OUTPUT_VERSION}.log" &
        sleep 120
    fi
    ind=$(($INDEX + 1))
    if (( ind <= $MAX_SUBSET_INDEX)); then
        echo "Starting sentence extraction pipeline... ${ind} $(date)"
        COLLECTION="${SUBSET_PREFIX}${ind}"
        OUTPUT_COLLECTION=$COLLECTION
        $SCRIPT $PROJECT_ID ${COLLECTION} $TEXT_PIPELINE_KEY $TEXT_PIPELINE_VERSION $SECTION_PIPELINE_KEY $SECTION_PIPELINE_VERSION $SENTENCE_PIPELINE_KEY $SENTENCE_PIPELINE_VERSION $CONCEPT_POST_PROCESS_VERSION $ASSOCIATION $PREFIX_X $PLACEHOLDER_X $PREFIX_Y $PLACEHOLDER_Y ${STAGE_LOCATION} ${TEMP_LOCATION} ${WORK_BUCKET} $JAR_VERSION ${OUTPUT_VERSION} ${OUTPUT_COLLECTION} &> "./logs/sentence-extraction-${COLLECTION}-${ASSOCIATION}-${OUTPUT_VERSION}.log" &
        sleep 120
    fi
    ind=$(($INDEX + 2))
    if (( ind <= $MAX_SUBSET_INDEX)); then
        echo "Starting sentence extraction pipeline... ${ind} $(date)"
        COLLECTION="${SUBSET_PREFIX}${ind}"
        OUTPUT_COLLECTION=$COLLECTION
        $SCRIPT $PROJECT_ID ${COLLECTION} $TEXT_PIPELINE_KEY $TEXT_PIPELINE_VERSION $SECTION_PIPELINE_KEY $SECTION_PIPELINE_VERSION $SENTENCE_PIPELINE_KEY $SENTENCE_PIPELINE_VERSION $CONCEPT_POST_PROCESS_VERSION $ASSOCIATION $PREFIX_X $PLACEHOLDER_X $PREFIX_Y $PLACEHOLDER_Y ${STAGE_LOCATION} ${TEMP_LOCATION} ${WORK_BUCKET} $JAR_VERSION ${OUTPUT_VERSION} ${OUTPUT_COLLECTION} &> "./logs/sentence-extraction-${COLLECTION}-${ASSOCIATION}-${OUTPUT_VERSION}.log" &
        sleep 120
    fi
    ind=$(($INDEX + 3))
    if (( ind <= $MAX_SUBSET_INDEX)); then
        echo "Starting sentence extraction pipeline... ${ind} $(date)"
        COLLECTION="${SUBSET_PREFIX}${ind}"
        OUTPUT_COLLECTION=$COLLECTION
        $SCRIPT $PROJECT_ID ${COLLECTION} $TEXT_PIPELINE_KEY $TEXT_PIPELINE_VERSION $SECTION_PIPELINE_KEY $SECTION_PIPELINE_VERSION $SENTENCE_PIPELINE_KEY $SENTENCE_PIPELINE_VERSION $CONCEPT_POST_PROCESS_VERSION $ASSOCIATION $PREFIX_X $PLACEHOLDER_X $PREFIX_Y $PLACEHOLDER_Y ${STAGE_LOCATION} ${TEMP_LOCATION} ${WORK_BUCKET} $JAR_VERSION ${OUTPUT_VERSION} ${OUTPUT_COLLECTION} &> "./logs/sentence-extraction-${COLLECTION}-${ASSOCIATION}-${OUTPUT_VERSION}.log" &
    fi
    wait 
  done
