#!/bin/bash

java -cp /home/code/tm-pipelines-bundled-0.1.0.jar edu.cuanschutz.ccp.tm_provider.relation_extraction.annot_batch_cli.ElasticsearchToBratExporterCLI $@
