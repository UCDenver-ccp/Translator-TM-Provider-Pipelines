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
gzip -dc *.txt.gz > /home/turku/all.txt
popd

# call the Turku dependency parser here
cat /home/turku/all.txt | python3 tnpp_parse.py --conf models_craft_dia/pipelines.yaml parse_plaintext > /home/turku/all.conllu
[ $? -eq 0 ] || exit 1

# compress the conllu file
gzip /home/turku/all.conllu
[ $? -eq 0 ] || exit 1

# export the all.conllu file to a GCP bucket
gsutil cp /home/turku/all.conllu.gz "${OUTPUT_BUCKET}/output/dependency_parses/${COLLECTION}/${COLLECTION}.${BATCH}.dependency_parses.conllu.gz"
[ $? -eq 0 ] || exit 1