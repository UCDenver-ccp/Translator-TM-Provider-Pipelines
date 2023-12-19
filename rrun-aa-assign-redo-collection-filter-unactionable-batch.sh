#!/usr/local/bin/bash


doesn't seem to be working for some reason
Exception in thread "main" java.lang.IllegalArgumentException: must have at least two input to a PCollectionList
        at org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument(Preconditions.java:141)
        at org.apache.beam.sdk.transforms.Sets$SetImplCollections.expand(Sets.java:629)
        at org.apache.beam.sdk.transforms.Sets$SetImplCollections.expand(Sets.java:608)
        at org.apache.beam.sdk.Pipeline.applyInternal(Pipeline.java:548)
        at org.apache.beam.sdk.Pipeline.applyTransform(Pipeline.java:499)
        at org.apache.beam.sdk.values.PCollectionList.apply(PCollectionList.java:192)
        at edu.cuanschutz.ccp.tm_provider.etl.CollectionAssignmentPipeline.main(CollectionAssignmentPipeline.java:147)
        at edu.cuanschutz.ccp.tm_provider.etl.PipelineMain.main(PipelineMain.java:145)

source ./rrun.env.sh

SCRIPT=./scripts/pipelines/collections/run_collection_assignment.sh

INPUT_DOCUMENT_CRITERIA="ACTIONABLE_TEXT|TEXT|FILTER_UNACTIONABLE_TEXT|0.1.0"
INPUT_COLLECTION=PMCOA
TARGET_PROCESSING_STATUS_FLAG=FILTER_UNACTIONABLE_DONE

OUTPUT_COLLECTION=REDO_FILTER_UNACT_20231129

$SCRIPT $INPUT_DOCUMENT_CRITERIA $INPUT_COLLECTION $OUTPUT_COLLECTION $TARGET_PROCESSING_STATUS_FLAG $PROJECT_ID ${STAGE_LOCATION} ${TEMP_LOCATION} $JAR_VERSION &> "./logs/collection-assignment-${TARGET_PROCESSING_STATUS_FLAG}-${OUTPUT_COLLECTION}.log" &
