#!/bin/sh

PROJECT=$1
COLLECTION=$2
STAGE_LOCATION=$3
TMP_LOCATION=$4
BUCKET=$5


JOB_NAME=$(echo "SENTENCE-EXTRACTION-PR-PR-REG-${COLLECTION}" | tr '_' '-')


echo "COLLECTION: $COLLECTION"
echo "PROJECT: $PROJECT"
echo "JOB_NAME: $JOB_NAME"

UPREGULATION='increase|increased|increases|increasing|amplify|amplified|amplifies|amplifying|raise|raises|raised|raising|multiply|multiplied|multiplies|multiplying|elevate|elevates|elevated|elevating|augment|augments|augmented|positive|stimulate|stimulates|stimulated|stimulating|activate|activates|activated|activating|up-regulate|up-regulates|up-regulated|up-regulating|upregulate|upregulates|upregulated|upregulating|transactivate|transactivates|transactivated|transactivating|trans-activate|trans-activates|trans-activated|trans-activating|catalyze|catalyzes|catalyzed|catalyzing|reactivate|reactivates|reactivated|reactivating|transduce|transduces|transduced|transducing|enhance|enhances|enhanced|enhancing|activator|coactivator|co-activator|upregulator|up-regulator|transducer|activation|up-regulation|upregulation|transactivation|promote|promotes|promoted|promoting|evoke|evokes|evoked|evoking|enhance|enhances|enhanced|enhancing|stabilize|stabilized|stabilizes|stabilizing|augment|augments|augmented|augmenting|facilitate|facilitates|facilitated|facilitating|trigger|triggers|triggered|triggering|potentiate|potentiates|potentiated|potentiating|elevate|elevates|elevated|elevating|raise|raises|raised|raising|stimulation|stimulator|elevation|rise|elevator'
DOWNREGULATION='decrease|decreases|decreased|decreasing|reduce|reduces|reduced|reducing|lower|lowers|lowered|lowering|repress|represses|repressing|repressed|negative|depress|depressed|depresses|depressing|block|blocks|blocked|blocking|inactivate|inactivates|inactivated|inactivating|suppress|suppresses|suppressed|suppressing|inhibit|inhibits|inhibited|inhibiting|downregulate|downregulates|downregulated|downregulating|down-regulate|down-regulates|down-regulated|down-regulating|antagonize|antagonizes|antagonized|antagonizing|deactivate|deactivates|deactivating|deactivated|inhibitor|inhibition|repression|suppression|down-regulation|downregulation|degradation|inactivation|repressor|suppressor|downregulator|down-regulator|disrupt|disrupts|disrupted|disrupting|compete|competes|competed|competing|interfere|interferes|interfered|interfering|block|blocks|blocked|blocking|attenuate|attenuates|attenuated|attenuating|decrease|decreases|decreased|decreasing|prevent|prevents|prevented|preventing|abolish|abolishes|abolished|abolishing|abrogate|abrogates|abrogated|abrogating|impair|impairs|impaired|impairing|degrade|degrades|degraded|degrading|diminish|diminishes|diminished|diminishing|destabilize|destabilizes|destabilizing|destabilized|restrict|restricts|restricted|restricting|competition|reduction|sequestration|blocking|restriction'
NONSPECIFIC_REGULATION='induce|induces|induced|inducing|regulate|regulates|regulated|regulating|target|targets|targeted|targeting|phosphorylate|phosphorylates|phosphorylated|phosphorylating|acetylate|acetylates|acetylated|acetylating|demethylate|demethylates|demethylated|demethylating|hydrolyse|hydrolyses|hydrolysed|hydrolysing|ubiquitinate|ubiquitinates|ubiquitinated|ubiquitinating|polyubiquitinate|polyubiquitinates|polyubiquitinated|polyubiquitinating|dephosphorylate|dephosphorylates|dephosphorylated|dephosphorylating|deacetylate|deacetylates|deacetylated|deacetylating|methylate|methylates|methylated|methylating|sumoylate|sumoylates|sumoylated|sumoylating|cleave|cleaves|cleaved|cleaving|recognize|recognizes|recognized|recognizing|recognise|recognises|recognised|recognising|oxidize|oxidizes|oxidized|oxidizing|target|targets|targeted|targeting|induction|regulator|regulation|acetylation|methylation|phosphorylation|demethylation|ubiquitination|deubiquitination|monoubiquitination|polyubiquitination|dephosphorylation|deacetylation|cleavage|recognition|mediate|mediates|mediated|mediating|influence|influences|influenced|influencing|recruit|recruits|recruited|recruiting|modulate|modulates|modulated|modulating|modulation|modulator|modifier|modification|control|controls|controlled|controlling|modify|modifies|modified|modifying|affect|affects|affecting|affected|require|requires|required|requiring|recruitment|bind|binds|bound|binding|interact|interacts|interacted|interacting|ligand|interplay|heterodimerize|heterodimerizes|heterodimerized|heterodimerizing|homodimerize|homodimerizes|homodimerized|homodimerizing|bond|conjugate|dimerize|dimerizes|dimerized|dimerizing|react|reacts|reacted|reacting|complex|complexes|complexed|co-immunoprecipitate|co-immunoprecipitates|co-immunoprecipitated|co-immunoprecipitating|coimmunoprecipitate|coimmunoprecipitates|coimmunoprecipitated|coimmunoprecipitating|assemble|assembles|assembled|assembling|ligate|ligates|ligated|ligating|ligase|tether|tethers|tethered|tethering|receptor|coupling|couple|couples|coupled|interaction|interplay|reaction|heterodimer|homodimer|ligation|conjugation|heterodimerization|homodimerization|docking|substrate|interactor|contact|contacts|contacted|contacting|link|links|linked|linking|associate|associates|associated|associating|cooperate|cooperates|cooperated|cooperating|correlate|correlates|correlating|correlated|synergize|synergizes|synergized|synergizing|synergise|synergises|synergised|synergising|relate|relates|related|relating|cooperation|cross-talk|association|coassociation|boundary|cross-reactivity|cross-regulation|effect|effects|effected|effecting|formation|linkage|signaling|response|attachment|attach|attaches|attached|attaching|anchoring'

OUTPUT_BUCKET="${BUCKET}/extracted-sentences/pr-pr/pr-pr-reg"

java -Dfile.encoding=UTF-8 -jar target/tm-pipelines-bundled-0.1.0.jar SENTENCE_EXTRACTION \
--jobName=$JOB_NAME \
--targetProcessingStatusFlag='SENTENCE_DONE' \
--inputDocumentCriteria='TEXT|TEXT|MEDLINE_XML_TO_TEXT|0.1.0;SENTENCE|BIONLP|SENTENCE_SEGMENTATION|0.1.0;CONCEPT_ALL|BIONLP|CONCEPT_POST_PROCESS|0.1.0' \
--keywords=$NONSPECIFIC_REGULATION \
--collection=$COLLECTION \
--overwrite='YES' \
--outputBucket=${OUTPUT_BUCKET} \
--prefixX='PR' \
--placeholderX='@PROTEIN$' \
--prefixY='PR' \
--placeholderY='@PROTEIN$' \
--project=${PROJECT} \
--stagingLocation=$STAGE_LOCATION \
--gcpTempLocation=$TMP_LOCATION \
--zone=us-central1-c \
--numWorkers=10 \
--maxNumWorkers=125 \
--runner=DataflowRunner
#--workerMachineType=n1-highmem-2 \