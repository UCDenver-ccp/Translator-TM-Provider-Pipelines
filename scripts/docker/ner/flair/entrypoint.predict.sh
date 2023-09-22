#!/bin/bash
input_bucket=$1
output_bucket=$2

# download CoNLL03 files to process
gsutil -m cp $input_bucket/*.txt.gz /home/flair/input_conll03 

python predict.py

# upload processed CoNLL03 files
gsutil -m cp /home/flair/output_conll03/*.conll03.gz $output_bucket