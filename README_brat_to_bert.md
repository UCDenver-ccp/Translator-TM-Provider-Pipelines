# BRAT -to- BERT

This page details instructions for using the brat_to_bert.Dockerfile to convert manually annotated sentences in the BRAT format into training data for BERT models.



### Use the image to convert annotated sentences in the BRAT format to BERT format

```
docker run --rm -v /path/to/brat/directory/on/local/host/:/brat_files -v /path/to/output/directory/where/bert/file/will/be/created/:/bert_files ucdenverccp/brat-to-bert:0.1 [BIOLINK_ASSOCIATION_NAME] [RECURSE] [OUTPUT_FILE_NAME]
```
where,
* `/path/to/brat/directory/on/local/host/` is the path on the local machine where the BRAT files to process are located
* `/path/to/output/directory/where/bert/file/will/be/created/` is the path on the local machine where the BERT file will be created
* `[BIOLINK_ASSOCIATION_NAME]` is the name of the association that has been annotated in the BRAT files. It must match the names of the [BiolinkAssociation enum](https://github.com/UCDenver-ccp/Translator-TM-Provider-Pipelines/blob/f7b82b38556a98a75e9ff5448f294d14934b22e3/src/main/java/edu/cuanschutz/ccp/tm_provider/etl/util/BiolinkConstants.java#L22), e.g. `bl_chemical_to_disease_or_phenotypic_feature`.
* `[RECURSE]` is YES or NO, indicating if the process should recurse through the BRAT file directory structure
* `[OUTPUT_FILE_NAME]` is the name of the output file that will contain training data for BERT models after calling the `docker run` command.


### Build the image
Note that the image has been published on [Dockerhub](https://hub.docker.com/repository/docker/ucdenverccp/brat-to-bert). If you want to build it locally however, run the following command from the base directory of this project:

```bash
docker build -t brat-to-bert -f brat_to_bert.Dockerfile .
```