#!/bin/sh

PROJECT=$1
COLLECTION=$2
STAGE_LOCATION=$3
TMP_LOCATION=$4
BUCKET=$5
TEXT_PIPELINE_KEY=$6
TEXT_PIPELINE_VERSION=$7
OGER_PIPELINE_VERSION=$8
CRF_PIPELINE_VERSION=$9
FILTER_FLAG=${10}


JOB_NAME=$(echo "CONCEPT-POST-PROCESS-${COLLECTION}" | tr '_' '-')

# if filter flag is BY_CRF then we need to collect the CRF annotations as well as the concept annotations and the target processing status flag = CONCEPT_POST_PROCESSING_DONE
# if the filter flag is NONE then we don't need the CRF annotations and the target processing status flag = CONCEPT_POST_PROCESSING_UNFILTERED_DONE
if [ $FILTER_FLAG == 'BY_CRF' ]
then
	INPUT_DOC_CRITERIA="TEXT|TEXT|${TEXT_PIPELINE_KEY}|${TEXT_PIPELINE_VERSION};CONCEPT_CIMAX|BIONLP|OGER|${OGER_PIPELINE_VERSION};CONCEPT_CIMIN|BIONLP|OGER|${OGER_PIPELINE_VERSION};CONCEPT_CS|BIONLP|OGER|${OGER_PIPELINE_VERSION};CRF_CHEBI|BIONLP|CRF|${CRF_PIPELINE_VERSION};CRF_PR|BIONLP|CRF|${CRF_PIPELINE_VERSION};CRF_CL|BIONLP|CRF|${CRF_PIPELINE_VERSION};CRF_UBERON|BIONLP|CRF|${CRF_PIPELINE_VERSION};CRF_GO_BP|BIONLP|CRF|${CRF_PIPELINE_VERSION};CRF_GO_CC|BIONLP|CRF|${CRF_PIPELINE_VERSION};CRF_GO_MF|BIONLP|CRF|${CRF_PIPELINE_VERSION};CRF_SO|BIONLP|CRF|${CRF_PIPELINE_VERSION};CRF_NCBITAXON|BIONLP|CRF|${CRF_PIPELINE_VERSION};CRF_HP|BIONLP|CRF|${CRF_PIPELINE_VERSION};CRF_MONDO|BIONLP|CRF|${CRF_PIPELINE_VERSION}"
	TARGET_PROCESSING_STATUS_FLAG=CONCEPT_POST_PROCESSING_DONE
else
	INPUT_DOC_CRITERIA="TEXT|TEXT|${TEXT_PIPELINE_KEY}|${TEXT_PIPELINE_VERSION};CONCEPT_CIMAX|BIONLP|OGER|${OGER_PIPELINE_VERSION};CONCEPT_CIMIN|BIONLP|OGER|${OGER_PIPELINE_VERSION};CONCEPT_CS|BIONLP|OGER|${OGER_PIPELINE_VERSION}"
	TARGET_PROCESSING_STATUS_FLAG=CONCEPT_POST_PROCESSING_UNFILTERED_DONE
fi

echo "COLLECTION: $COLLECTION"
echo "PROJECT: $PROJECT"
echo "JOB_NAME: $JOB_NAME"
echo "FILTER_FLAG: $FILTER_FLAG"
echo "TARGET_PROCESSING_STATUS_FLAG: $TARGET_PROCESSING_STATUS_FLAG"
echo "INPUT_DOC_CRITERIA: $INPUT_DOC_CRITERIA"

REQUIRED_FLAGS='TEXT_DONE'

PR_PROMOTION_MAP_FILE_PATH="${BUCKET}/ontology-resources/pr-promotion-map.tsv.gz"
NCBITAXON_PROMOTION_MAP_FILE_PATH="${BUCKET}/ontology-resources/ncbitaxon-promotion-map.tsv.gz"
EXTENSION_MAP_FILE_PATH="${BUCKET}/ontology-resources/craft-mapping-files/*.txt.gz"
#ANCESTOR_MAP_FILE_PATH="${BUCKET}/ontology-resources/ontology-class-ancestor-map.tsv.gz"

java -Dfile.encoding=UTF-8 -jar target/tm-pipelines-bundled-0.2.0.jar CONCEPT_POST_PROCESS \
--jobName="$JOB_NAME" \
--inputDocumentCriteria="$INPUT_DOC_CRITERIA" \
--requiredProcessingStatusFlags="$REQUIRED_FLAGS" \
--prPromotionMapFilePath="$PR_PROMOTION_MAP_FILE_PATH" \
--prPromotionMapFileDelimiter='TAB' \
--ncbiTaxonPromotionMapFilePath="$NCBITAXON_PROMOTION_MAP_FILE_PATH" \
--ncbiTaxonPromotionMapFileDelimiter='TAB' \
--ncbiTaxonPromotionMapFileSetDelimiter='PIPE' \
--extensionMapFilePath="$EXTENSION_MAP_FILE_PATH" \
--extensionMapFileDelimiter='TAB' \
--extensionMapFileSetDelimiter='TAB' \
--collection="$COLLECTION" \
--overwrite='YES' \
--filterFlag="$FILTER_FLAG" \
--targetProcessingStatusFlag="$TARGET_PROCESSING_STATUS_FLAG" \
--project="${PROJECT}" \
--stagingLocation="$STAGE_LOCATION" \
--gcpTempLocation="$TMP_LOCATION" \
--workerZone=us-central1-c \
--region=us-central1 \
--numWorkers=30 \
--maxNumWorkers=75 \
--autoscalingAlgorithm=THROUGHPUT_BASED \
--workerMachineType=n1-highmem-2 \
--runner=DataflowRunner


#--ancestorMapFilePath="$ANCESTOR_MAP_FILE_PATH" \
#--ancestorMapFileDelimiter='TAB' \
#--ancestorMapFileSetDelimiter='PIPE' \