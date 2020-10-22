package edu.cuanschutz.ccp.tm_provider.relation_extraction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import edu.cuanschutz.ccp.tm_provider.kg.KgxEdge;
import edu.cuanschutz.ccp.tm_provider.kg.KgxEdge.EvidenceMode;
import edu.cuanschutz.ccp.tm_provider.kg.KgxNlpEvidenceNode;
import edu.cuanschutz.ccp.tm_provider.kg.KgxNode;
import edu.cuanschutz.ccp.tm_provider.kg.KgxUtil;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;
import edu.ucdenver.ccp.nlp.core.annotation.Span;

public class BertOutputToKgx {

	private static final String KGX_NODE_FILE_HEADER = KgxUtil.KGX_NODE_WITH_EVIDENCE_HEADER;
	private static final String KGX_EDGE_FILE_HEADER = KgxUtil.KGX_EDGE_WITH_EVIDENCE_HEADER;

	private static final String BIOLINK_ASSOCIATION = "biolink:ChemicalToGeneAssociation";
	private static final String POSITIVELY_REGULATES_EDGE_LABEL = "biolink:positively_regulates_entity_to_entity";
	private static final String NEGATIVELY_REGULATES_EDGE_LABEL = "biolink:negatively_regulates_entity_to_entity";
	private static final String EVIDENCE_NODE_CATEGORY = "biolink:InformationContentEntity";
	private static final String POSITIVELY_REGULATES_RELATION = "RO:0002213";
	private static final String NEGATIVELY_REGULATES_RELATION = "RO:0002212";
	private static final String PROVIDED_BY = "TMProvider";
	private static final String CATEGORY_GENE_OR_GENE_PRODUCT = "biolink:GeneOrGeneProduct";
	private static final String CATEGORY_CHEMICAL_SUBSTANCE = "biolink:ChemicalSubstance";

	public static void toKgx(File bertPredictionsFile, File outputDir, Map<String, String> curieToLabelMap)
			throws IOException {

		Map<String, Set<String>> edgeIdToEvidenceIdMap = new HashMap<String, Set<String>>();
		Set<String> alreadyWrittenNodeIds = new HashSet<String>();

		File kgxNodesFile = new File(outputDir, "nodes.kgx.tsv.gz");
		// a temporary edges file is generated first, and the evidence node ids are
		// subsequently inserted in a second pass
		File kgxEdgesTmpFile = new File(outputDir, "edges.TMP.kgx.tsv.gz");
		File kgxEdgesFile = new File(outputDir, "edges.kgx.tsv.gz");

		try (BufferedWriter edgeWriter = FileWriterUtil.initBufferedWriter(
				new GZIPOutputStream(new FileOutputStream(kgxEdgesTmpFile)), CharacterEncoding.UTF_8);
				BufferedWriter nodeWriter = FileWriterUtil.initBufferedWriter(
						new GZIPOutputStream(new FileOutputStream(kgxNodesFile)), CharacterEncoding.UTF_8)) {
			edgeWriter.write(KGX_EDGE_FILE_HEADER + "\n");
			nodeWriter.write(KGX_NODE_FILE_HEADER + "\n");

			int nodeColumnCount = KGX_NODE_FILE_HEADER.split("\\t").length;
			System.out.println("NODE COL COUNT: " + nodeColumnCount);

			int count = 0;
			for (StreamLineIterator lineIter = new StreamLineIterator(
					new GZIPInputStream(new FileInputStream(bertPredictionsFile)), CharacterEncoding.UTF_8,
					null); lineIter.hasNext();) {

				if (count++ % 10000 == 0) {
					System.out.println("kgx serialization progress: " + (count - 1));
				}
				String line = lineIter.next().getText();
				String[] cols = line.split("\\t");

				String regulationType = cols[0];
				String edgeLabel = getEdgeLabel(regulationType);
				String relation = getRelation(regulationType);
				double bertScore = Double.parseDouble(cols[1]);
				// cols[2] = sentence id
				// cols[3] = masked sentence
				String documentId = cols[4];
				// cols[5] = protein covered text
				String proteinCurie = cols[6];
				List<Span> proteinSpans = getSpans(cols[7]);
				// cols[8] = chemical covered text
				String chemicalCurie = cols[9];
				List<Span> chemicalSpans = getSpans(cols[10]);
				// cols[11] = sentence search word
				// cols[12] = sentence length
				// cols[13] = blank
				String sentence = cols[14];

				String posneg = getPosNegLabel(regulationType);
				KgxEdge edge = new KgxEdge(BIOLINK_ASSOCIATION, chemicalCurie, edgeLabel, proteinCurie, relation);

				// since these were classified by bert, we don't know what the trigger word is
				List<Span> triggerSpans = Collections.emptyList();
				String evidenceNodeName = String.format("Evidence: %s -%s-reg-> %s", chemicalCurie, posneg,
						proteinCurie);
				KgxNlpEvidenceNode evidenceNode = new KgxNlpEvidenceNode(edge.getId(), evidenceNodeName,
						EVIDENCE_NODE_CATEGORY, documentId, bertScore, sentence, chemicalSpans, triggerSpans,
						proteinSpans, PROVIDED_BY);

				String chemicalLabel = curieToLabelMap.get(chemicalCurie);
				String proteinLabel = curieToLabelMap.get(proteinCurie);

				if (chemicalLabel == null || proteinLabel == null) {
					throw new IllegalArgumentException(
							"Null label for: " + ((chemicalLabel == null) ? chemicalCurie : proteinCurie));
				}

				KgxNode chemicalNode = new KgxNode(chemicalCurie, chemicalLabel, CATEGORY_CHEMICAL_SUBSTANCE);
				KgxNode proteinNode = new KgxNode(proteinCurie, proteinLabel, CATEGORY_GENE_OR_GENE_PRODUCT);

				// evidence nodes will be unique, so no need to track whether they have already
				// been written
				if (!alreadyWrittenNodeIds.contains(evidenceNode.getId())) {
					alreadyWrittenNodeIds.add(evidenceNode.getId());
					nodeWriter.write(evidenceNode.toKgxString(nodeColumnCount) + "\n");
				}
				writeNode(chemicalNode, nodeWriter, nodeColumnCount, alreadyWrittenNodeIds);
				writeNode(proteinNode, nodeWriter, nodeColumnCount, alreadyWrittenNodeIds);
				writeEdge(edge, edgeWriter, edgeIdToEvidenceIdMap);

				// store the edge-to-evidence id info
				CollectionsUtil.addToOne2ManyUniqueMap(edge.getId(), evidenceNode.getId(), edgeIdToEvidenceIdMap);

			}
		}

		// now fill in evidence nodes for edges
		try (BufferedWriter edgeWriter = FileWriterUtil.initBufferedWriter(
				new GZIPOutputStream(new FileOutputStream(kgxEdgesFile)), CharacterEncoding.UTF_8)) {
			edgeWriter.write(KGX_EDGE_FILE_HEADER + "\n");

			int count = 0;
			StreamLineIterator lineIter = new StreamLineIterator(
					new GZIPInputStream(new FileInputStream(kgxEdgesTmpFile)), CharacterEncoding.UTF_8, null);
			// burn header line
			lineIter.next();

			while (lineIter.hasNext()) {
				if (count++ % 10000 == 0) {
					System.out.println("kgx evidence replacement progress: " + (count - 1));
				}

				String line = lineIter.next().getText();

				String[] cols = line.split("\\t");
				String edgeId = cols[4];

				String evidenceStr = serializeEvidenceNodeIds(edgeIdToEvidenceIdMap, edgeId);
				int evidenceCount = edgeIdToEvidenceIdMap.get(edgeId).size();

				// replace placeholder with the evidence node ids
				line = line.replace(KgxEdge.EVIDENCE_ID_PLACEHOLDER, evidenceStr);
				line = line.replace(KgxEdge.EVIDENCE_COUNT_PLACEHOLDER, Integer.toString(evidenceCount));

				edgeWriter.write(line + "\n");
			}
		}

		KgxUtil.validateFile(kgxNodesFile, KGX_NODE_FILE_HEADER);
		KgxUtil.validateFile(kgxEdgesFile, KGX_EDGE_FILE_HEADER);

	}

	private static String serializeEvidenceNodeIds(Map<String, Set<String>> edgeIdToEvidenceIdMap, String edgeId) {
		StringBuilder sb = new StringBuilder();
		Set<String> evidenceNodeIds = edgeIdToEvidenceIdMap.get(edgeId);
		for (String evidenceNodeId : evidenceNodeIds) {
			sb.append(evidenceNodeId + "|");
		}
		// remove trailing pipe
		sb.deleteCharAt(sb.length() - 1);
		String evidenceStr = sb.toString();
		return evidenceStr;
	}

	private static void writeEdge(KgxEdge edge, BufferedWriter edgeWriter,
			Map<String, Set<String>> edgeIdToEvidenceIdMap) throws IOException {
		// if the edge has already been written, it will have an entry in the
		// edgeIdToEvidenceIdMap
		if (!edgeIdToEvidenceIdMap.containsKey(edge.getId())) {
			edgeWriter.write(edge.toKgxString(EvidenceMode.WRITE_PLACEHOLDER) + "\n");
		}
	}

	/**
	 * writes the node to file if it hasn't already been written
	 * 
	 * @param node
	 * @param writer
	 * @param nodeColumnCount
	 * @param alreadyWrittenNodeIds
	 * @throws IOException
	 */
	private static void writeNode(KgxNode node, BufferedWriter writer, int nodeColumnCount,
			Set<String> alreadyWrittenNodeIds) throws IOException {
		if (!alreadyWrittenNodeIds.contains(node.getId())) {
			alreadyWrittenNodeIds.add(node.getId());
			writer.write(node.toKgxString(nodeColumnCount) + "\n");
		}
	}

	/**
	 * parse span from string, e.g. [[108..113]]
	 * 
	 * @param string
	 * @return
	 */
	private static List<Span> getSpans(String spanStr) {
		List<Span> spansToReturn = new ArrayList<Span>();
		// drop first and last brackets
		String s = spanStr.substring(1, spanStr.length() - 1);
		String[] spans = s.split(",");
		for (String span : spans) {
			// drop brackets
			span = span.substring(1, span.length() - 1);
			String[] toks = span.split("\\.\\.");
			int spanStart = Integer.parseInt(toks[0]);
			int spanEnd = Integer.parseInt(toks[1]);
			spansToReturn.add(new Span(spanStart, spanEnd));
		}
		return spansToReturn;
	}

	private static String getPosNegLabel(String regulationType) {
		String label = null;
		if (regulationType.equals("UP")) {
			label = "pos";
		} else if (regulationType.equals("DOWN")) {
			label = "neg";
		}
		if (label == null) {
			throw new IllegalArgumentException(
					"Unexpected regulation type. Expected UP or DOWN but observed: " + regulationType);
		}
		return label;
	}

	private static String getRelation(String regulationType) {
		String relation = null;
		if (regulationType.equals("UP")) {
			relation = POSITIVELY_REGULATES_RELATION;
		} else if (regulationType.equals("DOWN")) {
			relation = NEGATIVELY_REGULATES_RELATION;
		}
		if (relation == null) {
			throw new IllegalArgumentException(
					"Unexpected regulation type. Expected UP or DOWN but observed: " + regulationType);
		}
		return relation;
	}

	private static String getEdgeLabel(String regulationType) {
		String edgeLabel = null;
		if (regulationType.equals("UP")) {
			edgeLabel = POSITIVELY_REGULATES_EDGE_LABEL;
		} else if (regulationType.equals("DOWN")) {
			edgeLabel = NEGATIVELY_REGULATES_EDGE_LABEL;
		}
		if (edgeLabel == null) {
			throw new IllegalArgumentException(
					"Unexpected regulation type. Expected UP or DOWN but observed: " + regulationType);
		}
		return edgeLabel;
	}

	private static Map<String, String> loadLabelMap(File outputDir, File ontologyDir, File craftOntologyDir)
			throws IOException, ClassNotFoundException, OWLOntologyCreationException {
		// see if it has been serialized, otherwise load from ontologies
		File serializedMapFile = new File(outputDir, "labelMap.ser");
		if (serializedMapFile.exists()) {
			return loadSeriallizedMap(serializedMapFile);
		}

		// it doesn't exist on disk, so create it
		Map<String, String> curieToLabelMap = populateLabelMap(ontologyDir, craftOntologyDir);

		// serialize map so that it can be loaded quickly in the future
		serializeMap(serializedMapFile, curieToLabelMap);

		return curieToLabelMap;
	}

	private static void serializeMap(File serializedMapFile, Map<String, String> curieToLabelMap)
			throws FileNotFoundException, IOException {
		System.out.println("Saving label map to file...");
		FileOutputStream fos = new FileOutputStream(serializedMapFile);
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		oos.writeObject(curieToLabelMap);
		oos.close();
		fos.close();
	}

	private static Map<String, String> loadSeriallizedMap(File serializedMapFile)
			throws FileNotFoundException, IOException, ClassNotFoundException {
		System.out.println("Loading label map from file...");
		FileInputStream fis = new FileInputStream(serializedMapFile);
		ObjectInputStream ois = new ObjectInputStream(fis);
		HashMap<String, String> curieToLabelMap = (HashMap) ois.readObject();
		ois.close();
		fis.close();
		return curieToLabelMap;
	}

	private static Map<String, String> populateLabelMap(File ontologyDir, File craftOntologyDir)
			throws OWLOntologyCreationException, FileNotFoundException, IOException {
		Map<String, String> curieToLabelMap = new HashMap<String, String>();

		File ontologyFile = new File(ontologyDir, "pr.owl.gz");
		addLabelMappings(curieToLabelMap, ontologyFile);
		ontologyFile = new File(ontologyDir, "chebi.owl.gz");
		addLabelMappings(curieToLabelMap, ontologyFile);
		ontologyFile = new File(craftOntologyDir, "PR.obo.gz");
		addLabelMappings(curieToLabelMap, ontologyFile);
		ontologyFile = new File(craftOntologyDir, "CHEBI.obo.gz");
		addLabelMappings(curieToLabelMap, ontologyFile);

		return curieToLabelMap;
	}

	private static void addLabelMappings(Map<String, String> curieToLabelMap, File ontologyFile)
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

				curieToLabelMap.put(getId(cls), label);

			}
		}
	}

	/**
	 * Convert from full URI to PREFIX:00000
	 * 
	 * @param cls
	 * @param replacedEXT
	 * @return
	 */
	private static String getId(OWLClass cls) {
		String iri = cls.getIRI().toString();
		int index = iri.lastIndexOf("/") + 1;
		return iri.substring(index).replace("_", ":");
	}

	public static void main(String[] args) {
//		File bertPredictionsFile = new File(
//				"/Users/bill/projects/ncats-translator/biolink-text-mining/relation-extraction/chebi-pr/chebi-pr-sentences/tagged_results.tsv.gz");

		File bertPredictionsFile = new File(
				"/Users/bill/projects/ncats-translator/biolink-text-mining/relation-extraction/chebi-pr/chebi-pr-sentences/tagged-chebi-pr-sentences.filtered.test_results.tsv.gz");

		File outputDir = new File(
				"/Users/bill/projects/ncats-translator/biolink-text-mining/relation-extraction/chebi-pr/chebi-pr-sentences/kgx");

		File ontologyDir = new File("/Users/bill/projects/ncats-translator/ontology-resources/ontologies");
		File craftOntologyDir = new File("/Users/bill/projects/ncats-translator/ontology-resources/ontologies/craft");

		try {
			Map<String, String> curieToLabelMap = loadLabelMap(outputDir, ontologyDir, craftOntologyDir);
			toKgx(bertPredictionsFile, outputDir, curieToLabelMap);
		} catch (IOException | ClassNotFoundException | OWLOntologyCreationException e) {
			e.printStackTrace();
		}
	}

}
