#!/bin/bash

# for each collection we will process the plain text files in multiple batches. This parameter is a unique identifier for a given batch.
BATCH=$1

# the name of the collection being processed - this will be part of the output file name
COLLECTION=$2

# TXT_FILE_BUCKET is getting passed in at runtime, i.e. during docker run
# This should be the full gs://bucket/a/b/c path to where the aggregated plain text files are located
TXT_FILE_BUCKET=$3

# the gcp bucket where the dependency parse files will be stored
OUTPUT_BUCKET=$4

# download the txt files to process
# cat the txt files into a single file called all.tsv in /home/turku
mkdir /home/turku/txt
pushd /home/turku/txt
gsutil cp "$TXT_FILE_BUCKET/*.txt.gz" .
cat ./*.txt.gz > /home/turku/all.txt.gz
popd

# call the Turku dependency parser here
gunzip -c /home/turku/all.txt.gz | python3 full_pipeline_stream.py --conf-yaml models_en_ewt/pipelines.yaml parse_plaintext | gzip > /home/turku/all.conllu.gz
[ $? -eq 0 ] || exit 1

# export the all.conllu.gz file to a GCP bucket
gsutil cp /home/turku/all.conllu.gz "${OUTPUT_BUCKET}/${COLLECTION}.${BATCH}.dependency_parses.conllu.gz"
[ $? -eq 0 ] || exit 1