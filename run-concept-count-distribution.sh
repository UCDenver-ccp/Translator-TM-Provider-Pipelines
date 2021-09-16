#!/bin/sh


PROJECT_ID=translator-text-workflow-dev
BUCKET=gs://translator-text-workflow-dev_work

WORK_BUCKET=gs://translator-text-workflow-dev_work
STAGE_LOCATION=$WORK_BUCKET/staging 
TEMP_LOCATION=$WORK_BUCKET/temp


./scripts/pipelines/metrics/run_concept_count_distribution.sh ${PROJECT_ID} ${STAGE_LOCATION} ${TEMP_LOCATION} ${BUCKET} &> ./logs/concept-count-distribution.log &

