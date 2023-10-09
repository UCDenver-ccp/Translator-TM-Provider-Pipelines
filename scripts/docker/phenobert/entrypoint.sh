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
mkdir /home/code/input
pushd /home/code/input
gsutil cp "$TXT_FILE_BUCKET/*.txt.gz" .
cat ./*.txt.gz > /home/code/input/all.txt.gz
popd

# call PhenoBERT - this will write a file to /home/code/output/phenobert.bionlp.gz
python process-input-file.py
[ $? -eq 0 ] || exit 1

# export the phenobert.bionlp.gz file to a GCP bucket
gsutil cp /home/code/output/phenobert.bionlp.gz "${OUTPUT_BUCKET}/${COLLECTION}.${BATCH}.phenobert.bionlp.gz"
[ $? -eq 0 ] || exit 1