#!/bin/bash
#
# monitors an AI Platform job by periodically interrogating the job log. 
# Continues to monitor while the state is 'RUNNING', 'QUEUED', or 'PREPARING'. 
# Fails if the state is 'FAILED'. Succeeds if the state is 'COMPLETED'.
#

JOB_ID=$1

STATE="QUEUED"
until [ "$STATE" == "FAILED" ] || [ "$STATE" == "SUCCEEDED" ]
do
    sleep 30
    STATE=$(gcloud ai-platform jobs describe $JOB_ID | grep state | cut -f 2 -d " ")
    echo "STATE: $STATE"
done

if [ "$STATE" == "FAILED" ]
then
    exit -1
fi
