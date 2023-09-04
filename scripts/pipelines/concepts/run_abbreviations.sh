#!/bin/sh

AB3P_SOURCE_PATH=$1
WORKER_PATH=$2
TEXT_PIPELINE_KEY=$3
TEXT_PIPELINE_VERSION=$4
SENTENCE_PIPELINE_VERSION=$5
PROJECT=$6
COLLECTION=$7
STAGE_LOCATION=$8
TMP_LOCATION=$9
JAR_VERSION=${10}
OVERWRITE=${11}

JOB_NAME=$(echo "ABBREVIATION-${COLLECTION}" | tr '_' '-')

echo "AB3P_SOURCE_PATH: $AB3P_SOURCE_PATH"
echo "WORKER_PATH: $WORKER_PATH"
echo "TEXT_PIPELINE_KEY: $TEXT_PIPELINE_KEY"
echo "TEXT_PIPELINE_VERSION: $TEXT_PIPELINE_VERSION"
echo "COLLECTION: $COLLECTION"
echo "PROJECT: $PROJECT"
echo "JOB_NAME: $JOB_NAME"

java -Dfile.encoding=UTF-8 -jar target/tm-pipelines-bundled-${JAR_VERSION}.jar ABBREVIATION \
--jobName="$JOB_NAME" \
--abbreviationBinaryName="identify_abbr" \
--binaryFileAndDependencies="identify_abbr|path_Ab3P|WordData/Ab3P_prec.dat|WordData/Lf1chSf|WordData/SingTermFreq.dat|WordData/cshset_wrdset3.ad|WordData/cshset_wrdset3.ct|WordData/cshset_wrdset3.ha|WordData/cshset_wrdset3.nm|WordData/cshset_wrdset3.str|WordData/hshset_Lf1chSf.ad|WordData/hshset_Lf1chSf.ha|WordData/hshset_Lf1chSf.nm|WordData/hshset_Lf1chSf.str|WordData/hshset_stop.ad|WordData/hshset_stop.ha|WordData/hshset_stop.nm|WordData/hshset_stop.str|WordData/stop" \
--sourcePath="$AB3P_SOURCE_PATH" \
--workerPath="$WORKER_PATH" \
--concurrency=1 \
--inputTextPipelineKey="$TEXT_PIPELINE_KEY" \
--inputTextPipelineVersion="$TEXT_PIPELINE_VERSION" \
--inputSentencePipelineKey='SENTENCE_SEGMENTATION' \
--inputSentencePipelineVersion="${SENTENCE_PIPELINE_VERSION}" \
--collection="$COLLECTION" \
--overwrite="$OVERWRITE" \
--project="${PROJECT}" \
--stagingLocation="$STAGE_LOCATION" \
--gcpTempLocation="$TMP_LOCATION" \
--workerZone=us-central1-c \
--region=us-central1 \
--numWorkers=10 \
--maxNumWorkers=75 \
--autoscalingAlgorithm=THROUGHPUT_BASED \
--runner=DataflowRunner