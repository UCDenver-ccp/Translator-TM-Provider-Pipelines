#!/bin/sh

PROJECT=$1
COLLECTION=$2
STAGE_LOCATION=$3
TMP_LOCATION=$4


JOB_NAME=$(echo "SENTENCE-EXTRACTION-${COLLECTION}" | tr '_' '-')


echo "COLLECTION: $COLLECTION"
echo "PROJECT: $PROJECT"
echo "JOB_NAME: $JOB_NAME"


UPREGULATION='increase|increased|increases|increasing|amplify|amplified|amplifies|amplifying|raise|raises|raised|raising|multiply|multiplied|multiplies|multiplying|elevate|elevates|elevated|elevating|augment|augments|augmented|positive|stimulate|stimulates|stimulated|stimulating|activate|activates|activated|activating|up-regulate|up-regulates|up-regulated|up-regulating|upregulate|upregulates|upregulated|upregulating|transactivate|transactivates|transactivated|transactivating|trans-activate|trans-activates|trans-activated|trans-activating|catalyze|catalyzes|catalyzed|catalyzing|reactivate|reactivates|reactivated|reactivating|transduce|transduces|transduced|transducing|enhance|enhances|enhanced|enhancing|activator|coactivator|co-activator|upregulator|up-regulator|transducer|activation|up-regulation|upregulation|transactivation|promote|promotes|promoted|promoting|evoke|evokes|evoked|evoking|enhance|enhances|enhanced|enhancing|stabilize|stabilized|stabilizes|stabilizing|augment|augments|augmented|augmenting|facilitate|facilitates|facilitated|facilitating|trigger|triggers|triggered|triggering|potentiate|potentiates|potentiated|potentiating|elevate|elevates|elevated|elevating|raise|raises|raised|raising|stimulation|stimulator|elevation|rise|elevator'
DOWNREGULATION='decrease|decreases|decreased|decreasing|reduce|reduces|reduced|reducing|lower|lowers|lowered|lowering|repress|represses|repressing|repressed|negative|depress|depressed|depresses|depressing|block|blocks|blocked|blocking|inactivate|inactivates|inactivated|inactivating|suppress|suppresses|suppressed|suppressing|inhibit|inhibits|inhibited|inhibiting|downregulate|downregulates|downregulated|downregulating|down-regulate|down-regulates|down-regulated|down-regulating|antagonize|antagonizes|antagonized|antagonizing|deactivate|deactivates|deactivating|deactivated|inhibitor|inhibition|repression|suppression|down-regulation|downregulation|degradation|inactivation|repressor|suppressor|downregulator|down-regulator|disrupt|disrupts|disrupted|disrupting|compete|competes|competed|competing|interfere|interferes|interfered|interfering|block|blocks|blocked|blocking|attenuate|attenuates|attenuated|attenuating|decrease|decreases|decreased|decreasing|prevent|prevents|prevented|preventing|abolish|abolishes|abolished|abolishing|abrogate|abrogates|abrogated|abrogating|impair|impairs|impaired|impairing|degrade|degrades|degraded|degrading|diminish|diminishes|diminished|diminishing|destabilize|destabilizes|destabilizing|destabilized|restrict|restricts|restricted|restricting|competition|reduction|sequestration|blocking|restriction'


java -Dfile.encoding=UTF-8 -jar target/tm-pipelines-bundled-0.1.0.jar SENTENCE_EXTRACTION \
--jobName=$JOB_NAME \
--targetProcessingStatusFlag='SENTENCE_DONE' \
--inputDocumentCriteria='TEXT|TEXT|MEDLINE_XML_TO_TEXT|0.1.0;SENTENCE|BIONLP|SENTENCE_SEGMENTATION|0.1.0;CONCEPT_CHEBI|BIONLP|OGER|0.1.0;CRF_CHEBI|BIONLP|CRF|0.1.0;CONCEPT_PR|BIONLP|OGER|0.1.0;CRF_PR|BIONLP|CRF|0.1.0' \
--keywords=$DOWNREGULATION \
--collection=$COLLECTION \
--overwrite='YES' \
--outputBucket='gs://translator-tm-provider-datastore-staging-stage/extracted-sentences/chebi-pr-downregulation' \
--project=${PROJECT} \
--stagingLocation=$STAGE_LOCATION \
--gcpTempLocation=$TMP_LOCATION \
--zone=us-central1-c \
--numWorkers=10 \
--maxNumWorkers=100 \
--workerMachineType=n1-highmem-2 \
--runner=DataflowRunner