#!/bin/bash

BIOLINK_ASSOC=$1
RECURSE=$2
BERT_FILE_NAME=$3

mvn exec:java -Dexec.mainClass="edu.cuanschutz.ccp.tm_provider.relation_extraction.BratToBertConverterMain" -Dexec.args="${BIOLINK_ASSOC} /brat_files ${RECURSE} /bert_files/${BERT_FILE_NAME}"