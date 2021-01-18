package edu.cuanschutz.ccp.tm_provider.kg.ontology_kg;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import edu.cuanschutz.ccp.tm_provider.kg.KgxEdge;
import edu.cuanschutz.ccp.tm_provider.kg.KgxEdge.EvidenceMode;
import edu.cuanschutz.ccp.tm_provider.kg.KgxNode;
import edu.cuanschutz.ccp.tm_provider.kg.KgxUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.string.StringUtil;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;

public class OntologyToKgx {

	private static final String BIOLINK_THING = "biolink:Thing";

	private static final Logger LOGGER = Logger.getLogger(OntologyToKgx.class.getName());

	private static final String KGX_NODE_FILE_HEADER = KgxUtil.KGX_NODE_HEADER;
	private static final String KGX_EDGE_FILE_HEADER = KgxUtil.KGX_EDGE_HEADER;

	private static final String BIOLINK_GENE = "biolink:Gene";

	private static final String BIOLINK_CHEMICAL_SUBSTANCE = "biolink:ChemicalSubstance";

	private static final String BIOLINK_CELL = "biolink:Cell";

	private static final String BIOLINK_PHENOTYPIC_FEATURE = "biolink:PhenotypicFeature";

	private static final String BIOLINK_DISEASE = "biolink:Disease";

	private static final String BIOLINK_MOLECULAR_ACTIVITY = "biolink:MolecularActivity";

	private static final String BIOLINK_ORGANISM_TAXON = "biolink:OrganismTaxon";

	private static final String BIOLINK_GENE_PRODUCT = "biolink:GeneProduct";

	private static final String BIOLINK_SEQUENCE_FEATURE = "biolink:SequenceFeature*";

	private static final String BIOLINK_ANATOMICAL_ENTITY = "biolink:AnatomicalEntity";

	private static final String BIOLINK_GENE_TO_GENE_ASSOCIATION = "biolink:GeneToGeneAssociation";

	private static final String BIOLINK_SAME_AS = "biolink:same_as";

	private static final String OWL_EQUIVALENT_CLASS = "owl:equivalentClass";

	private static final String OBI_ORGANISM = "http://purl.obolibrary.org/obo/OBI_0100026";

	private static final String BIOLINK_ORGANSIMAL_ENTITY = "biolink:OrganismalEntity";

	private static final String BIOLINK_GENE_TO_GENE_PRODUCT_RELATIONSHIP_ASSOCIATION = "biolink:GeneToGeneProductRelationship";

	private static final String BIOLINK_GENE_TO_THING_ASSOCIATION = "biolink:GeneToThingAssociation";

	private static final String BIOLINK_CHEMICAL_TO_GENE_ASSOCIATION = "biolink:ChemicalToGeneAssociation";

	private static final String BIOLINK_ASSOCIATION = "biolink:Association";

	private static Map<String, String> getConceptPrefixMap() {
		Map<String, String> map = new HashMap<String, String>();
		map.put("http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=", "HGNC");
		map.put("http://www.informatics.jax.org/marker/MGI:", "MGI");
		map.put("http://www.ncbi.nlm.nih.gov/gene/", "NCBIGene");
		map.put("http://purl.obolibrary.org/obo/PR_", "PR");
		map.put("http://purl.obolibrary.org/obo/CHEBI_", "CHEBI");
		map.put("http://purl.obolibrary.org/obo/GO_", "GO");
		map.put("http://purl.obolibrary.org/obo/CL_", "CL");
		map.put("http://purl.obolibrary.org/obo/MOP_", "MOP");
		map.put("http://purl.obolibrary.org/obo/MONDO_", "MONDO");
		map.put("http://purl.obolibrary.org/obo/HP_", "HP");
		map.put("http://purl.obolibrary.org/obo/NCBITaxon_", "NCBITaxon");
		map.put("http://purl.obolibrary.org/obo/SO_", "SO");
		map.put("http://purl.obolibrary.org/obo/UBERON_", "UBERON");
		map.put("http://www.yeastgenome.org/cgi-bin/locus.fpl?dbid=", "SGD");
		map.put("http://www.wormbase.org/species/c_elegans/gene/", "WormBase");
		map.put("http://flybase.org/reports/", "FlyBase");
		map.put("http://zfin.org/action/marker/view/", "ZFIN");
		map.put("http://dictybase.org/gene/", "dictyBase");
		map.put("https://www.araport.org/locus/", "TAIR");
		map.put("http://www.pombase.org/spombe/result/", "PomBase");
		map.put("http://rgd.mcw.edu/rgdweb/report/gene/main.html?id=", "RGD");
		map.put("http://www.ecogene.org/gene/", "EcoGene");
		map.put("http://purl.obolibrary.org/obo/EnsemblBacteria#_", "EnsembleBacteria");
		map.put("http://birdgenenames.org/cgnc/GeneReport?id=", "CGNC");
		map.put("http://www.ensemblgenomes.org/id/", "EnsemblGene");
		map.put("http://www.ensembl.org/id/", "Ensembl");
		map.put("http://purl.obolibrary.org/obo/Ensembl#_", "Ensembl");
		map.put("http://purl.obolibrary.org/obo/BFO_", "BFO");
		map.put("http://purl.obolibrary.org/obo/OBI_", "OBI");
		map.put("http://purl.obolibrary.org/obo/MOD_", "MOD");
		return map;
	}

	private static Map<String, String> getRelationToBiolinkMap() {
		Map<String, String> map = new HashMap<String, String>();
		map.put("http://purl.obolibrary.org/obo/RO_0002180", "biolink:has_component*"); // has_component
		map.put("http://purl.obolibrary.org/obo/RO_0002160", "biolink:in_taxon"); // only_in_taxon
		map.put("http://purl.obolibrary.org/obo/pr#lacks_part", "biolink:lacks_part*");
		map.put("SUBCLASS_OF", "biolink:subClassOf");
		map.put("http://purl.obolibrary.org/obo/pr#non-covalently_bound_to", "biolink:non_covalently_bound_to*");
		map.put("http://purl.obolibrary.org/obo/RO_0002353", "biolink:output_of*"); // output_of
		map.put("http://purl.obolibrary.org/obo/RO_0001000", "biolink:derives_from"); // derives_from
		map.put("http://purl.obolibrary.org/obo/RO_0002331", "biolink:participates_in"); // involved_in
		map.put("http://purl.obolibrary.org/obo/RO_0000086", "biolink:has_quality*"); // has_quality
		map.put("http://purl.obolibrary.org/obo/pr#has_gene_template", "biolink:has_gene_template*");
		map.put("http://purl.obolibrary.org/obo/BFO_0000050", "biolink:part_of"); // part_of
		map.put("http://purl.obolibrary.org/obo/RO_0001025", "biolink:located_in"); // located_in
		map.put("http://purl.obolibrary.org/obo/RO_0002215", "biolink:capable_of"); // capable_of
		map.put("http://purl.obolibrary.org/obo/BFO_0000051", "biolink:has_part"); // has_part
		return map;
	}

	private static Map<String, String> getRelationToCurieMap() {
		Map<String, String> map = new HashMap<String, String>();
		map.put("http://purl.obolibrary.org/obo/RO_0002180", "RO:0002180"); // has_component
		map.put("http://purl.obolibrary.org/obo/RO_0002160", "RO:0002160"); // only_in_taxon
		map.put("http://purl.obolibrary.org/obo/pr#lacks_part", "PR:lacks_part");
		map.put("SUBCLASS_OF", "rdfs:subClassOf");
		map.put("http://purl.obolibrary.org/obo/pr#non-covalently_bound_to", "PR:non_covalently_bound_to");
		map.put("http://purl.obolibrary.org/obo/RO_0002353", "RO:0002353"); // output_of
		map.put("http://purl.obolibrary.org/obo/RO_0001000", "RO:0001000"); // derives_from
		map.put("http://purl.obolibrary.org/obo/RO_0002331", "RO:0002331"); // involved_in
		map.put("http://purl.obolibrary.org/obo/RO_0000086", "RO:0000086"); // has_quality
		map.put("http://purl.obolibrary.org/obo/pr#has_gene_template", "PR:has_gene_template");
		map.put("http://purl.obolibrary.org/obo/BFO_0000050", "BFO:0000050"); // part_of
		map.put("http://purl.obolibrary.org/obo/RO_0001025", "RO:0001025"); // located_in
		map.put("http://purl.obolibrary.org/obo/RO_0002215", "RO:0002215"); // capable_of
		map.put("http://purl.obolibrary.org/obo/BFO_0000051", "BFO:0000051"); // has_part
		return map;
	}

	private static Map<String, String> getAssociationMap() {
		Map<String, String> map = new HashMap<String, String>();

		map.put("biolink:GeneProduct -- biolink:Thing -- http://purl.obolibrary.org/obo/pr#lacks_part",
				BIOLINK_GENE_TO_THING_ASSOCIATION);
		map.put("biolink:GeneProduct -- biolink:GeneProduct -- http://purl.obolibrary.org/obo/RO_0002180",
				BIOLINK_GENE_TO_GENE_ASSOCIATION);
		map.put("biolink:GeneProduct -- biolink:OrganismTaxon -- http://purl.obolibrary.org/obo/RO_0002160",
				BIOLINK_GENE_TO_THING_ASSOCIATION);
		map.put("biolink:Thing -- biolink:GeneProduct -- SUBCLASS_OF", BIOLINK_GENE_TO_THING_ASSOCIATION);
		map.put("biolink:SequenceFeature* -- biolink:Thing -- SUBCLASS_OF", BIOLINK_ASSOCIATION);
		map.put("biolink:GeneProduct -- biolink:GeneProduct -- http://purl.obolibrary.org/obo/pr#has_gene_template",
				BIOLINK_GENE_TO_GENE_PRODUCT_RELATIONSHIP_ASSOCIATION);
		map.put("biolink:GeneProduct -- biolink:ChemicalSubstance -- http://purl.obolibrary.org/obo/BFO_0000051",
				BIOLINK_CHEMICAL_TO_GENE_ASSOCIATION);
		map.put("biolink:GeneProduct -- biolink:SequenceFeature* -- SUBCLASS_OF", BIOLINK_GENE_TO_THING_ASSOCIATION);
		map.put("biolink:GeneProduct -- biolink:Gene -- http://purl.obolibrary.org/obo/pr#has_gene_template",
				BIOLINK_GENE_TO_GENE_PRODUCT_RELATIONSHIP_ASSOCIATION);
		map.put("biolink:GeneProduct -- biolink:SequenceFeature* -- http://purl.obolibrary.org/obo/pr#has_gene_template",
				BIOLINK_GENE_TO_THING_ASSOCIATION);
		map.put("biolink:BiologicalProcess -- biolink:Thing -- SUBCLASS_OF", BIOLINK_ASSOCIATION);
		map.put("biolink:Cell -- biolink:Cell -- SUBCLASS_OF", BIOLINK_ASSOCIATION);
		map.put("biolink:OrganismTaxon -- biolink:OrganismalEntity -- SUBCLASS_OF", BIOLINK_ASSOCIATION);
		map.put("biolink:GeneProduct -- biolink:BiologicalProcess -- http://purl.obolibrary.org/obo/RO_0002353",
				BIOLINK_GENE_TO_THING_ASSOCIATION);
		map.put("biolink:GeneProduct -- biolink:BiologicalProcess -- http://purl.obolibrary.org/obo/RO_0002331",
				BIOLINK_GENE_TO_THING_ASSOCIATION);
		map.put("biolink:OrganismalEntity -- biolink:Thing -- SUBCLASS_OF", BIOLINK_ASSOCIATION);
		map.put("biolink:SequenceFeature* -- biolink:GeneProduct -- SUBCLASS_OF", BIOLINK_GENE_TO_THING_ASSOCIATION);
		map.put("biolink:GeneProduct -- biolink:GeneProduct -- http://purl.obolibrary.org/obo/pr#lacks_part",
				BIOLINK_GENE_TO_GENE_ASSOCIATION);
		map.put("biolink:BiologicalProcess -- biolink:BiologicalProcess -- SUBCLASS_OF", BIOLINK_ASSOCIATION);
		map.put("biolink:Gene -- biolink:OrganismTaxon -- http://purl.obolibrary.org/obo/RO_0002160",
				BIOLINK_GENE_TO_THING_ASSOCIATION);
		map.put("biolink:GeneProduct -- biolink:CellularComponent -- http://purl.obolibrary.org/obo/BFO_0000050",
				BIOLINK_GENE_TO_THING_ASSOCIATION);
		map.put("biolink:GeneProduct -- biolink:ChemicalSubstance -- SUBCLASS_OF", BIOLINK_GENE_TO_THING_ASSOCIATION);
		map.put("biolink:Gene -- biolink:SequenceFeature* -- SUBCLASS_OF", BIOLINK_GENE_TO_THING_ASSOCIATION);
		map.put("biolink:GeneProduct -- biolink:CellularComponent -- SUBCLASS_OF", BIOLINK_GENE_TO_THING_ASSOCIATION);
		map.put("biolink:GeneProduct -- biolink:ChemicalSubstance -- http://purl.obolibrary.org/obo/pr#non-covalently_bound_to",
				BIOLINK_CHEMICAL_TO_GENE_ASSOCIATION);
		map.put("biolink:CellularComponent -- biolink:CellularComponent -- SUBCLASS_OF", BIOLINK_ASSOCIATION);
		map.put("biolink:GeneProduct -- biolink:GeneProduct -- http://purl.obolibrary.org/obo/BFO_0000050",
				BIOLINK_GENE_TO_GENE_ASSOCIATION);
		map.put("biolink:Thing -- biolink:Thing -- SUBCLASS_OF", BIOLINK_ASSOCIATION);
		map.put("biolink:CellularComponent -- biolink:Thing -- SUBCLASS_OF", BIOLINK_ASSOCIATION);
		map.put("biolink:GeneProduct -- biolink:SequenceFeature* -- http://purl.obolibrary.org/obo/pr#lacks_part",
				BIOLINK_GENE_TO_THING_ASSOCIATION);
		map.put("biolink:GeneProduct -- biolink:GeneProduct -- http://purl.obolibrary.org/obo/RO_0001000",
				BIOLINK_GENE_TO_GENE_ASSOCIATION);
		map.put("biolink:GeneProduct -- biolink:GeneProduct -- SUBCLASS_OF", BIOLINK_GENE_TO_GENE_ASSOCIATION);
		map.put("biolink:OrganismTaxon -- biolink:OrganismTaxon -- SUBCLASS_OF", BIOLINK_ASSOCIATION);
		map.put("biolink:GeneProduct -- biolink:SequenceFeature* -- http://purl.obolibrary.org/obo/BFO_0000051",
				BIOLINK_GENE_TO_THING_ASSOCIATION);
		map.put("biolink:GeneProduct -- biolink:GeneProduct -- http://purl.obolibrary.org/obo/BFO_0000051",
				BIOLINK_GENE_TO_GENE_ASSOCIATION);
		map.put("biolink:GeneProduct -- biolink:Thing -- http://purl.obolibrary.org/obo/BFO_0000051",
				BIOLINK_GENE_TO_THING_ASSOCIATION);
		map.put("biolink:ChemicalSubstance -- biolink:GeneProduct -- SUBCLASS_OF", BIOLINK_GENE_TO_THING_ASSOCIATION);
		map.put("biolink:GeneProduct -- biolink:Cell -- http://purl.obolibrary.org/obo/BFO_0000050",
				BIOLINK_GENE_TO_THING_ASSOCIATION);
		map.put("biolink:GeneProduct -- biolink:ChemicalSubstance -- http://purl.obolibrary.org/obo/RO_0002180",
				BIOLINK_CHEMICAL_TO_GENE_ASSOCIATION);
		map.put("biolink:ChemicalSubstance -- biolink:Thing -- SUBCLASS_OF", BIOLINK_ASSOCIATION);
		map.put("biolink:SequenceFeature* -- biolink:SequenceFeature* -- http://purl.obolibrary.org/obo/RO_0000086",
				BIOLINK_ASSOCIATION);
		map.put("biolink:GeneProduct -- biolink:MolecularActivity -- http://purl.obolibrary.org/obo/RO_0002215",
				BIOLINK_GENE_TO_THING_ASSOCIATION);
		map.put("biolink:SequenceFeature* -- biolink:SequenceFeature* -- SUBCLASS_OF", BIOLINK_ASSOCIATION);
		map.put("biolink:GeneProduct -- biolink:CellularComponent -- http://purl.obolibrary.org/obo/RO_0001025",
				BIOLINK_GENE_TO_THING_ASSOCIATION);
		map.put("biolink:GeneProduct -- biolink:Thing -- http://purl.obolibrary.org/obo/RO_0002180",
				BIOLINK_GENE_TO_THING_ASSOCIATION);
		map.put("biolink:GeneProduct -- biolink:Thing -- SUBCLASS_OF", BIOLINK_GENE_TO_THING_ASSOCIATION);
		map.put("biolink:ChemicalSubstance -- biolink:ChemicalSubstance -- SUBCLASS_OF", BIOLINK_ASSOCIATION);

		return map;
	}

	public enum Attribute {
		LABEL, CATEGORY, PREFIX, CURIE, UNIPROT_ID
	}

	public static void toKgx(File ontologyFile, File outputDir)
			throws OWLOntologyCreationException, FileNotFoundException, IOException, ClassNotFoundException {

		String outputFilePrefix = ontologyFile.getName();
		if (outputFilePrefix.endsWith(".gz")) {
			outputFilePrefix = StringUtil.removeSuffix(outputFilePrefix, ".gz");
		}

		File kgxNodesFile = new File(outputDir, String.format("%s-subclass-hierarchy.nodes.kgx.tsv.gz", outputFilePrefix));
		File kgxEdgesFile = new File(outputDir, String.format("%s-subclass-hierarchy.edges.kgx.tsv.gz", outputFilePrefix));

		int nodeColumnCount = KGX_NODE_FILE_HEADER.split("\\t").length;

		// outer key = IRI; inner key = relation name, inner value = target IRIs
		Map<String, Map<String, Set<String>>> outgoingRelation = loadOutgoingRelationsMap(outputDir, ontologyFile);

		// outer key = IRI; inner key = Attribue, inner value = Attribute value
		Map<String, Map<Attribute, String>> attributeMap = loadAttributeMap(outputDir, ontologyFile);

		for (Entry<String, Map<Attribute, String>> entry : attributeMap.entrySet()) {

			String iri = entry.getKey();
			Map<Attribute, String> attMap = entry.getValue();

			String category = attMap.get(Attribute.CATEGORY);

			if (category.equals(BIOLINK_THING) && iri.equals(OBI_ORGANISM)) {
				attMap.put(Attribute.CATEGORY, BIOLINK_ORGANSIMAL_ENTITY);
			}

			category = attMap.get(Attribute.CATEGORY);
			if (category.equals(BIOLINK_THING)) {
				System.out.println("Missing category: " + iri);
			}

			String prefix = attMap.get(Attribute.PREFIX);
			if (prefix == null) {
				prefix = getPrefix(iri);
				if (prefix != null) {
					attMap.put(Attribute.PREFIX, prefix);
				}
			}
			prefix = attMap.get(Attribute.PREFIX);
			if (prefix == null) {
				System.out.println("missing prefix: " + iri);
			}

			String curie = attMap.get(Attribute.CURIE);
			if (curie == null) {
				curie = getCurie(iri);
				attMap.put(Attribute.CURIE, curie);
			}
			curie = attMap.get(Attribute.CURIE);
			if (curie == null) {
				System.out.println("missing curie: " + iri);
			}

		}

		Set<String> relations = new HashSet<String>();
		for (Entry<String, Map<String, Set<String>>> entry : outgoingRelation.entrySet()) {
			Map<String, Set<String>> relationToTargetIriMap = entry.getValue();
			relations.addAll(relationToTargetIriMap.keySet());
		}

		for (String relation : relations) {
			if (!getRelationToBiolinkMap().containsKey(relation)) {
				System.out.println("MISSING RELATION: " + relation);
			}
		}

		Set<String> alreadyWrittenIds = new HashSet<String>();
		try (BufferedWriter edgeWriter = FileWriterUtil
				.initBufferedWriter(new GZIPOutputStream(new FileOutputStream(kgxEdgesFile)), CharacterEncoding.UTF_8);
				BufferedWriter nodeWriter = FileWriterUtil.initBufferedWriter(
						new GZIPOutputStream(new FileOutputStream(kgxNodesFile)), CharacterEncoding.UTF_8)) {
			edgeWriter.write(KGX_EDGE_FILE_HEADER + "\n");
			nodeWriter.write(KGX_NODE_FILE_HEADER + "\n");

			for (Entry<String, Map<String, Set<String>>> entry : outgoingRelation.entrySet()) {

				String sourceIri = entry.getKey();
				String sourceCurie = attributeMap.get(sourceIri).get(Attribute.CURIE);
				String sourceLabel = attributeMap.get(sourceIri).get(Attribute.LABEL);
				writeNode(nodeColumnCount, attributeMap, alreadyWrittenIds, nodeWriter, sourceIri);

				serializeUniprotPrSameAs(nodeColumnCount, attributeMap, alreadyWrittenIds, edgeWriter, nodeWriter,
						sourceIri, sourceCurie, sourceLabel);

				for (Entry<String, Set<String>> relationEntry : entry.getValue().entrySet()) {
					String relation = relationEntry.getKey();
					for (String targetIri : relationEntry.getValue()) {
						writeNode(nodeColumnCount, attributeMap, alreadyWrittenIds, nodeWriter, targetIri);
						String targetCurie = attributeMap.get(targetIri).get(Attribute.CURIE);
						String associationType = getAssociationType(sourceIri, targetIri, attributeMap, relation);

						if (associationType == null) {
							LOGGER.log(Level.WARNING,
									"Missing association type: " + sourceIri + " -- " + targetIri + " -- " + relation);
						}

						String edgeLabel = getRelationToBiolinkMap().get(relation);
						String relationCurie = getRelationToCurieMap().get(relation);
						KgxEdge edge = new KgxEdge(associationType, sourceCurie, edgeLabel, targetCurie, relationCurie);
						writeEdge(edge, edgeWriter, alreadyWrittenIds);
					}
				}
			}

		}

	}

	private static void serializeUniprotPrSameAs(int nodeColumnCount, Map<String, Map<Attribute, String>> attributeMap,
			Set<String> alreadyWrittenIds, BufferedWriter edgeWriter, BufferedWriter nodeWriter, String sourceIri,
			String sourceCurie, String sourceLabel) throws IOException {
		String uniprotId = attributeMap.get(sourceIri).get(Attribute.UNIPROT_ID);
		if (uniprotId != null) {
			KgxNode uniprotNode = new KgxNode(uniprotId, sourceLabel, BIOLINK_GENE_PRODUCT);
			writeNode(uniprotNode, nodeWriter, nodeColumnCount, alreadyWrittenIds);
			KgxEdge edge = new KgxEdge(BIOLINK_GENE_TO_GENE_ASSOCIATION, sourceCurie, BIOLINK_SAME_AS, uniprotId,
					OWL_EQUIVALENT_CLASS);
			writeEdge(edge, edgeWriter, alreadyWrittenIds);
		}
	}

	private static String getAssociationType(String sourceIri, String targetIri,
			Map<String, Map<Attribute, String>> attributeMap, String relation) {
		String sourceCategory = attributeMap.get(sourceIri).get(Attribute.CATEGORY);
		String targetCategory = attributeMap.get(targetIri).get(Attribute.CATEGORY);

		String key = sourceCategory + " -- " + targetCategory + " -- " + relation;

		return getAssociationMap().get(key);
	}

	private static void writeNode(int nodeColumnCount, Map<String, Map<Attribute, String>> attributeMap,
			Set<String> alreadyWrittenIds, BufferedWriter nodeWriter, String sourceIri) throws IOException {
		String sourceCurie = attributeMap.get(sourceIri).get(Attribute.CURIE);
		String sourceLabel = attributeMap.get(sourceIri).get(Attribute.LABEL);
		String sourceCategory = attributeMap.get(sourceIri).get(Attribute.CATEGORY);
		KgxNode node = new KgxNode(sourceCurie, sourceLabel, sourceCategory);
		writeNode(node, nodeWriter, nodeColumnCount, alreadyWrittenIds);
	}

	private static void writeNode(KgxNode node, BufferedWriter writer, int nodeColumnCount,
			Set<String> alreadyWrittenNodeIds) throws IOException {
		if (!alreadyWrittenNodeIds.contains(node.getId())) {
			alreadyWrittenNodeIds.add(node.getId());
			writer.write(node.toKgxString(nodeColumnCount) + "\n");
		}
	}

	private static void writeEdge(KgxEdge edge, BufferedWriter writer, Set<String> alreadyWrittenIds)
			throws IOException {
		if (!alreadyWrittenIds.contains(edge.getId())) {
			alreadyWrittenIds.add(edge.getId());
			writer.write(edge.toKgxString(EvidenceMode.WRITE_EVIDENCE) + "\n");
		}
	}

	private static Map<String, Map<String, Set<String>>> loadOutgoingRelationsMap(File outputDir, File ontologyFile)
			throws IOException, ClassNotFoundException, OWLOntologyCreationException {
		// see if it has been serialized, otherwise load from ontologies
		File serializedMapFile = new File(outputDir, "outgoingRelationsMap.ser");
		if (serializedMapFile.exists()) {
			return loadSerializedOutgoingRelationsMap(serializedMapFile);
		}

		// it doesn't exist on disk, so create it
		Map<String, Map<String, Set<String>>> iriToOutgoingRelationMap = populateOutgoingRelationMap(ontologyFile);

		// serialize map so that it can be loaded quickly in the future
		serializeOutgoingRelationsMap(serializedMapFile, iriToOutgoingRelationMap);

		return iriToOutgoingRelationMap;
	}

	/**
	 * map is IRI to a map from relation name to target IRI
	 * 
	 * @param ontologyFile
	 * @return
	 * @throws OWLOntologyCreationException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private static Map<String, Map<String, Set<String>>> populateOutgoingRelationMap(File ontologyFile)
			throws OWLOntologyCreationException, FileNotFoundException, IOException {
		Map<String, Map<String, Set<String>>> outputMap = new HashMap<String, Map<String, Set<String>>>();
		OntologyUtil ontUtil = new OntologyUtil(new GZIPInputStream(new FileInputStream(ontologyFile)));

		for (Iterator<OWLClass> clsIter = ontUtil.getClassIterator(); clsIter.hasNext();) {
			OWLClass cls = clsIter.next();
			Map<String, Set<String>> outgoingEdges = ontUtil.getOutgoingEdges(cls);

			outputMap.put(cls.getIRI().toString(), outgoingEdges);

		}
		return outputMap;
	}

	private static void serializeOutgoingRelationsMap(File serializedMapFile,
			Map<String, Map<String, Set<String>>> iriToOutgoingRelationsMap) throws FileNotFoundException, IOException {
		System.out.println("Saving outgoing relations map to file...");
		FileOutputStream fos = new FileOutputStream(serializedMapFile);
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		oos.writeObject(iriToOutgoingRelationsMap);
		oos.close();
		fos.close();
	}

	/**
	 * map is IRI to a map from relation name to target IRI
	 * 
	 * @param serializedMapFile
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private static Map<String, Map<String, Set<String>>> loadSerializedOutgoingRelationsMap(File serializedMapFile)
			throws FileNotFoundException, IOException, ClassNotFoundException {
		System.out.println("Loading outgoing relations map from file...");
		FileInputStream fis = new FileInputStream(serializedMapFile);
		ObjectInputStream ois = new ObjectInputStream(fis);
		@SuppressWarnings("unchecked")
		HashMap<String, Map<String, Set<String>>> uriToSuperClassUriMap = (HashMap<String, Map<String, Set<String>>>) ois
				.readObject();
		ois.close();
		fis.close();
		return uriToSuperClassUriMap;
	}

	private static Map<String, Map<Attribute, String>> loadAttributeMap(File outputDir, File ontologyFile)
			throws IOException, ClassNotFoundException, OWLOntologyCreationException {
		// see if it has been serialized, otherwise load from ontologies
		File serializedMapFile = new File(outputDir, "attributesMap.ser");
		if (serializedMapFile.exists()) {
			return loadSerializedAttributeMap(serializedMapFile);
		}

		// it doesn't exist on disk, so create it
		Map<String, Map<Attribute, String>> attributeMap = populateAttributeMap(ontologyFile);

		// serialize map so that it can be loaded quickly in the future
		serializeAttributeMap(serializedMapFile, attributeMap);

		return attributeMap;
	}

	private static void serializeAttributeMap(File serializedMapFile, Map<String, Map<Attribute, String>> attributeMap)
			throws FileNotFoundException, IOException {
		System.out.println("Saving label map to file...");
		FileOutputStream fos = new FileOutputStream(serializedMapFile);
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		oos.writeObject(attributeMap);
		oos.close();
		fos.close();
	}

	private static Map<String, Map<Attribute, String>> loadSerializedAttributeMap(File serializedMapFile)
			throws FileNotFoundException, IOException, ClassNotFoundException {
		System.out.println("Loading label map from file...");
		FileInputStream fis = new FileInputStream(serializedMapFile);
		ObjectInputStream ois = new ObjectInputStream(fis);
		@SuppressWarnings("unchecked")
		HashMap<String, Map<Attribute, String>> attributeMap = (HashMap<String, Map<Attribute, String>>) ois
				.readObject();
		ois.close();
		fis.close();
		return attributeMap;
	}

	private static Map<String, Map<Attribute, String>> populateAttributeMap(File ontologyFile)
			throws OWLOntologyCreationException, FileNotFoundException, IOException {
		Map<String, Map<Attribute, String>> attributeMap = new HashMap<String, Map<Attribute, String>>();
		addAttributes(attributeMap, ontologyFile);
		return attributeMap;
	}

	private static void addAttributes(Map<String, Map<Attribute, String>> attributeMap, File ontologyFile)
			throws OWLOntologyCreationException, IOException, FileNotFoundException {
		System.out.println("Processing " + ontologyFile.getName());
		OntologyUtil ontUtil = new OntologyUtil(new GZIPInputStream(new FileInputStream(ontologyFile)));

		int count = 0;
		for (Iterator<OWLClass> classIterator = ontUtil.getClassIterator(); classIterator.hasNext();) {
			if (count++ % 10000 == 0) {
				System.out.println(String.format("progress: %d...", count - 1));
			}
			OWLClass cls = classIterator.next();

			String label = ontUtil.getLabel(cls);

			if (label != null) {
				if (label.endsWith("\"")) {
					label = label.substring(0, label.length() - 1);
				}
			}

			String prefix = getPrefix(cls.getIRI().toString());
			String category = getCategory(ontUtil, cls, prefix);

			String uniprotId = getUniprotXRef(ontUtil, cls);

			Map<Attribute, String> map = new HashMap<Attribute, String>();
			map.put(Attribute.CATEGORY, category);
			map.put(Attribute.LABEL, label);
			map.put(Attribute.PREFIX, prefix);

			if (uniprotId != null) {
				map.put(Attribute.UNIPROT_ID, uniprotId);
			}

//			map.put(Attribute.CURIE, curie);

			attributeMap.put(cls.getIRI().toString(), map);

		}
	}

	private static String getUniprotXRef(OntologyUtil ontUtil, OWLClass cls) {
		Set<String> dbXrefs = ontUtil.getDbXrefs(cls);
		if (dbXrefs != null) {
			for (String xref : dbXrefs) {
				if (xref.startsWith("UniProtKB:")) {
					return xref;
				}
			}
		}
		return null;
	}

	private static String getCategory(OntologyUtil ontUtil, OWLClass cls, String prefix) {
		String category = null;

		if (isGene(cls, ontUtil)) {
			category = BIOLINK_GENE;
		}

		if (prefix != null) {
			switch (prefix) {
			case "GO":
				category = getGoCategory(ontUtil, cls);
				break;
			case "CHEBI":
				category = BIOLINK_CHEMICAL_SUBSTANCE;
				break;
			case "CL":
				category = BIOLINK_CELL;
				break;
			case "HGNC":
				category = BIOLINK_GENE;
				break;
			case "HP":
				category = BIOLINK_PHENOTYPIC_FEATURE;
				break;
			case "MONDO":
				category = BIOLINK_DISEASE;
				break;
			case "MOP":
				category = BIOLINK_MOLECULAR_ACTIVITY;
				break;
			case "NCBITaxon":
				category = BIOLINK_ORGANISM_TAXON;
				break;
			case "PR":
				category = BIOLINK_GENE_PRODUCT;
				break;
			case "SO":
				category = BIOLINK_SEQUENCE_FEATURE;
				break;
			case "UBERON":
				category = BIOLINK_ANATOMICAL_ENTITY;
				break;
			default:
				break;
			}
		}

		if (category == null) {
			category = BIOLINK_THING;
		}

		return category;

	}

	private static boolean isGene(OWLClass cls, OntologyUtil ontUtil) {
		Set<OWLClass> ancestors = ontUtil.getAncestors(cls);
		OWLClass geneCls = ontUtil.getOWLClassFromId("http://purl.obolibrary.org/obo/SO_0001217");
		return ancestors.contains(geneCls);
	}

	private static String getCurie(String iri) {
		for (Entry<String, String> entry : getConceptPrefixMap().entrySet()) {
			String namespace = entry.getKey();
			String prefix = entry.getValue();

			if (iri.startsWith(namespace)) {
				String id = StringUtil.removePrefix(iri, namespace);
				return prefix + ":" + id;
			}
		}
		return null;
	}

	private static String getPrefix(String iri) {
		for (Entry<String, String> entry : getConceptPrefixMap().entrySet()) {
			String namespace = entry.getKey();
			String prefix = entry.getValue();

			if (iri.startsWith(namespace)) {
				return prefix;
			}
		}
		return null;
	}

	private static String getGoCategory(OntologyUtil ontUtil, OWLClass cls) {
		String namespace = ontUtil.getNamespace(cls);
		if (namespace != null) {
			if (namespace.endsWith("\"")) {
				namespace = namespace.substring(0, namespace.length() - 1);
			}
			if (namespace.equals("biological_process")) {
				return "biolink:BiologicalProcess";
			} else if (namespace.equals("cellular_component")) {
				return "biolink:CellularComponent";
			} else if (namespace.equals("molecular_function")) {
				return "biolink:MolecularActivity";
			}
		}
		LOGGER.log(Level.WARNING, "Returning null category for GO concept: " + cls.getIRI().toString());
		return null;
	}

	public static void main(String[] args) {
		File ontologyFile = new File("/Users/bill/projects/ncats-translator/ontology-resources/ontologies/pr.owl.gz");
		File outputDir = new File("/Users/bill/projects/ncats-translator/ontology-resources/ontologies/kgx");
		try {

			toKgx(ontologyFile, outputDir);
		} catch (OWLOntologyCreationException | IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

}
