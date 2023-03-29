#!/bin/bash

JAVA_ARGS="$@"

# if the command is "batch" then add extra args
if [[ "$JAVA_ARGS" == batch* ]]; then
    JAVA_ARGS="${JAVA_ARGS} --go-bp-ids-file /home/data/gobp.ids --go-cc-ids-file /home/data/gocc.ids --concept-idf-file /home/data/concept_idf.csv.gz -d /home/input"
else
    # the command is stats, so we only need the -d parameter
    JAVA_ARGS="${JAVA_ARGS} -d /home/input"
fi

echo $JAVA_ARGS
java -cp /home/code/tm-pipelines-bundled-0.1.0.jar edu.cuanschutz.ccp.tm_provider.relation_extraction.annot_batch_cli.ElasticsearchToBratExporterCLI $JAVA_ARGS
