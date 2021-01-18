package edu.cuanschutz.ccp.tm_provider.trapi;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.reader.Line;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;

public class AssocKgTrapiBackendSerializer implements TrapiBackendSerializer {

	@Override
	public void serializeNodes(File nodeFile, BufferedWriter rdfWriter) throws IOException {

		for (StreamLineIterator lineIter = new StreamLineIterator(new GZIPInputStream(new FileInputStream(nodeFile)),
				CharacterEncoding.UTF_8, null); lineIter.hasNext();) {

			Line line = lineIter.next();
			if (line.getLineNumber() == 0) {
				// skip the header
				continue;
			}

			String[] cols = line.getText().split("\\t");

			String id = cols[0];
			String label = cols[1];
			String category = cols[2];

			// serialize entity node
			if (cols.length == 3) {
				String idWithUnderscore = id.replace(":", "_");
				Set<String> biolinkCategories = CollectionsUtil.createSet(BIOLINK_CHEMICAL_SUBSTANCE);
				if (id.startsWith("PR:")) {
					biolinkCategories = CollectionsUtil.createSet(BIOLINK_GENE_PRODUCT, BIOLINK_GENE_OR_GENE_PRODUCT);
				}
				/**
				 * <pre>
				 *  <http://purl.obolibrary.org/obo/CHEBI_3215> <https://w3id.org/biolink/vocab/id> "CHEBI:3215"@en .
				 * </pre>
				 */
				rdfWriter.write(String.format("%s%s> %s \"%s\"@en .\n", OBO_PREFIX, idWithUnderscore, BIOLINK_ID, id));

				/**
				 * <pre>
				 * <http://purl.obolibrary.org/obo/CHEBI_3215> <http://www.w3.org/2000/01/rdf-schema#label> "bupivacaine"@en .
				 * </pre>
				 */
				rdfWriter.write(
						String.format("%s%s> %s \"%s\"@en .\n", OBO_PREFIX, idWithUnderscore, RDFS_LABEL, label));

				/**
				 * <pre>
				 * <http://purl.obolibrary.org/obo/CHEBI_3215> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <https://w3id.org/biolink/vocab/ChemicalSubstance> .
				 * </pre>
				 */
				for (String biolinkCategory : biolinkCategories) {
					rdfWriter.write(String.format("%s%s> %s %s .\n", OBO_PREFIX, idWithUnderscore, RDFS_SUBCLASSOF,
							biolinkCategory));
				}
			} else {
				// then this is an evidence node

				String pmid = cols[3];
				String score = cols[4];
				String sentence = cols[5];
				String subject_spans = cols[6];
				String relation_spans = cols[7];
				String object_spans = cols[8];
				String provided_by = cols[9];

				String evidenceNodeId = String.format("<_:%s_evidence>", id);

				/**
				 * <pre>
				 *  <_:IjbFtUdgNQk-HHlsBju-I_jpSnA_evidence_0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://w3id.org/biolink/vocab/InformationContentEntity> .
				 * </pre>
				 */
				rdfWriter.write(
						String.format("%s %s %s .\n", evidenceNodeId, RDF_TYPE, BIOLINK_INFORMATION_CONTENT_ENTITY));

				/**
				 * <pre>
				 *  <_:IjbFtUdgNQk-HHlsBju-I_jpSnA_evidence_0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://w3id.org/biolink/vocab/TextMinedEvidence> .
				 * </pre>
				 */
				rdfWriter.write(String.format("%s %s %s .\n", evidenceNodeId, RDF_TYPE, BIOLINK_TEXT_MINED_EVIDENCE));

				/**
				 * <pre>
				 * 	<_:IjbFtUdgNQk-HHlsBju-I_jpSnA_evidence_0> <https://w3id.org/biolink/vocab/id> "02T60dgTsntC9kC4rqtr5lIN8n0"@en .
				 * </pre>
				 */
				rdfWriter.write(String.format("%s %s \"%s\"@en .\n", evidenceNodeId, BIOLINK_ID, id));

				/**
				 * <pre>
				 * <_:IjbFtUdgNQk-HHlsBju-I_jpSnA_evidence_0> <https://w3id.org/biolink/vocab/name> "Evidence: CHEBI:3215 -neg-reg-> PR:000031567"@en .
				 * </pre>
				 */
				rdfWriter.write(String.format("%s %s \"%s\"@en .\n", evidenceNodeId, BIOLINK_NAME, label));
				/**
				 * <pre>
				 * <_:IjbFtUdgNQk-HHlsBju-I_jpSnA_evidence_0> <https://w3id.org/biolink/vocab/publications> "PMID:29085514"@en .
				 * </pre>
				 */
				rdfWriter.write(String.format("%s %s \"%s\"@en .\n", evidenceNodeId, BIOLINK_PUBLICATIONS, pmid));
				/**
				 * <pre>
				 * <_:IjbFtUdgNQk-HHlsBju-I_jpSnA_evidence_0> <https://w3id.org/biolink/vocab/sentence> "The administration of 50 ?g/ml bupivacaine promoted maximum breast cancer cell invasion, and suppressed LRRC3B mRNA expression in cells."@en .
				 * </pre>
				 */
				rdfWriter.write(String.format("%s %s \"%s\"@en .\n", evidenceNodeId, BIOLINK_SENTENCE, sentence));
				/**
				 * <pre>
				 * <_:IjbFtUdgNQk-HHlsBju-I_jpSnA_evidence_0> <https://w3id.org/biolink/vocab/subject_spans> "start: 31, end: 42"@en .
				 * </pre>
				 */
				rdfWriter.write(
						String.format("%s %s \"%s\"@en .\n", evidenceNodeId, BIOLINK_SUBJECT_SPANS, subject_spans));
				/**
				 * <pre>
				 * <_:IjbFtUdgNQk-HHlsBju-I_jpSnA_evidence_0> <https://w3id.org/biolink/vocab/object_spans> "start: 104, end: 110"@en .
				 * </pre>
				 */
				rdfWriter.write(
						String.format("%s %s \"%s\"@en .\n", evidenceNodeId, BIOLINK_OBJECT_SPANS, object_spans));
				/**
				 * <pre>
				 * <_:IjbFtUdgNQk-HHlsBju-I_jpSnA_evidence_0> <https://w3id.org/biolink/vocab/provided_by> "TMProvider"@en .
				 * </pre>
				 */
				rdfWriter.write(String.format("%s %s \"%s\"@en .\n", evidenceNodeId, BIOLINK_PROVIDED_BY, provided_by));
				/**
				 * <pre>
				 * <_:IjbFtUdgNQk-HHlsBju-I_jpSnA_evidence_0> <https://w3id.org/biolink/vocab/score> "0.99956816"^^<http://www.w3.org/2001/XMLSchema#decimal> .
				 * </pre>
				 */
				rdfWriter.write(String.format("%s %s \"%s\"^^<http://www.w3.org/2001/XMLSchema#decimal> .\n",
						evidenceNodeId, BIOLINK_SCORE, score));

			}

		}

	}

	@Override
	public void serializeEdges(File edgeFile, BufferedWriter rdfWriter) throws IOException {

		for (StreamLineIterator lineIter = new StreamLineIterator(new GZIPInputStream(new FileInputStream(edgeFile)),
				CharacterEncoding.UTF_8, null); lineIter.hasNext();) {

			Line line = lineIter.next();
			if (line.getLineNumber() == 0) {
				// skip the header
				continue;
			}

			String[] cols = line.getText().split("\\t");

			int index = 0;
			String subjectId = cols[index++];
			String edgeLabel = cols[index++];
			String objectId = cols[index++];
			String relation = cols[index++];
			String id = cols[index++];
			String associationType = cols[index++];
			String evidenceCount = cols[index++];
			String hasEvidence = cols[index++];

			String subjInstanceId = String.format("<_:%s_subj>", id);
			String objInstanceId = String.format("<_:%s_obj>", id);
			String assocInstanceId = String.format("<_:%s_assoc>", id);

			String subjPurl = String.format("%s%s>", OBO_PREFIX, subjectId.replace(":", "_"));
			String objPurl = String.format("%s%s>", OBO_PREFIX, objectId.replace(":", "_"));

			String relationUri = null;
			switch (relation) {
			case "RO:0002212":
				relationUri = RO_0002212;
				break;
			case "RO:0002213":
				relationUri = RO_0002213;
				break;

			default:
				throw new IllegalArgumentException(
						String.format("Unhandled relation (%s). Code changes required.", relation));
			}

			/**
			 * <pre>
			 *  <_:IjbFtUdgNQk-HHlsBju-I_jpSnA_subj> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://w3id.org/biolink/vocab/ChemicalSubstance> .
			 * </pre>
			 */
			rdfWriter.write(String.format("%s %s %s .\n", subjInstanceId, RDF_TYPE, BIOLINK_CHEMICAL_SUBSTANCE));
			/**
			 * <pre>
			 *  <_:IjbFtUdgNQk-HHlsBju-I_jpSnA_subj> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.obolibrary.org/obo/CHEBI_3215> .
			 * </pre>
			 */
			rdfWriter.write(String.format("%s %s %s .\n", subjInstanceId, RDF_TYPE, subjPurl));
			/**
			 * <pre>
			 *  <_:IjbFtUdgNQk-HHlsBju-I_jpSnA_subj> <http://www.openrdf.org/schema/sesame#directType> <http://purl.obolibrary.org/obo/CHEBI_3215> .
			 * </pre>
			 */
			rdfWriter.write(String.format("%s %s %s .\n", subjInstanceId, DIRECT_TYPE, subjPurl));

			/**
			 * <pre>
			 *  <_:IjbFtUdgNQk-HHlsBju-I_jpSnA_obj> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://w3id.org/biolink/vocab/GeneProduct> .
			 * </pre>
			 */
			rdfWriter.write(String.format("%s %s %s .\n", objInstanceId, RDF_TYPE, BIOLINK_GENE_PRODUCT));
			/**
			 * <pre>
			 *  <_:IjbFtUdgNQk-HHlsBju-I_jpSnA_obj> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://w3id.org/biolink/vocab/GeneOrGeneProduct> .
			 * </pre>
			 */
			rdfWriter.write(String.format("%s %s %s .\n", objInstanceId, RDF_TYPE, BIOLINK_GENE_OR_GENE_PRODUCT));
			/**
			 * <pre>
			 *  <_:IjbFtUdgNQk-HHlsBju-I_jpSnA_obj> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.obolibrary.org/obo/PR_0000317567> .
			 * </pre>
			 */
			rdfWriter.write(String.format("%s %s %s .\n", objInstanceId, RDF_TYPE, objPurl));
			/**
			 * <pre>
			 *  <_:IjbFtUdgNQk-HHlsBju-I_jpSnA_obj> <http://www.openrdf.org/schema/sesame#directType> <http://purl.obolibrary.org/obo/PR_0000317567> .
			 * </pre>
			 */
			rdfWriter.write(String.format("%s %s %s .\n", objInstanceId, DIRECT_TYPE, objPurl));

			/**
			 * <pre>
			 *  <_:IjbFtUdgNQk-HHlsBju-I_jpSnA_subj> <http://purl.obolibrary.org/obo/RO_0002212> <_:IjbFtUdgNQk-HHlsBju-I_jpSnA_obj> .
			 * </pre>
			 */
			rdfWriter.write(String.format("%s %s %s .\n", subjInstanceId, relationUri, objInstanceId));
			/**
			 * <pre>
			 *  <_:IjbFtUdgNQk-HHlsBju-I_jpSnA_assoc> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://w3id.org/biolink/vocab/ChemicalToGeneAssociation> .
			 * </pre>
			 */
			rdfWriter.write(
					String.format("%s %s %s .\n", assocInstanceId, RDF_TYPE, BIOLINK_CHEMICAL_TO_GENE_ASSOCIATION));
			/**
			 * <pre>
			 *  <_:IjbFtUdgNQk-HHlsBju-I_jpSnA_assoc> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://w3id.org/biolink/vocab/Association> .
			 * </pre>
			 */
			rdfWriter.write(String.format("%s %s %s .\n", assocInstanceId, RDF_TYPE, BIOLINK_ASSOCIATION));
			/**
			 * <pre>
			 *  <_:IjbFtUdgNQk-HHlsBju-I_jpSnA_assoc> <https://w3id.org/biolink/vocab/subject> <_:IjbFtUdgNQk-HHlsBju-I_jpSnA_subj> .
			 * </pre>
			 */
			rdfWriter.write(String.format("%s %s %s .\n", assocInstanceId, BIOLINK_SUBJECT, subjInstanceId));
			/**
			 * <pre>
			 *  <_:IjbFtUdgNQk-HHlsBju-I_jpSnA_assoc> <https://w3id.org/biolink/vocab/object> <_:IjbFtUdgNQk-HHlsBju-I_jpSnA_obj> .
			 * </pre>
			 */
			rdfWriter.write(String.format("%s %s %s .\n", assocInstanceId, BIOLINK_OBJECT, objInstanceId));
			/**
			 * <pre>
			 *  <_:IjbFtUdgNQk-HHlsBju-I_jpSnA_assoc> <https://w3id.org/biolink/vocab/id> "IjbFtUdgNQk-HHlsBju-I_jpSnA"@en .
			 * </pre>
			 */
			rdfWriter.write(String.format("%s %s \"%s\"@en .\n", assocInstanceId, BIOLINK_ID, id));
			/**
			 * <pre>
			 *  <_:IjbFtUdgNQk-HHlsBju-I_jpSnA_assoc> <https://w3id.org/biolink/vocab/relation> <http://purl.obolibrary.org/obo/RO_0002212> .
			 * </pre>
			 */
			rdfWriter.write(String.format("%s %s %s .\n", assocInstanceId, BIOLINK_RELATION, relationUri));
			/**
			 * <pre>
			 *  <_:IjbFtUdgNQk-HHlsBju-I_jpSnA_assoc> <https://w3id.org/biolink/vocab/evidence> <_:IjbFtUdgNQk-HHlsBju-I_jpSnA_evidence_0> .
			 * </pre>
			 */
			for (String evidenceId : hasEvidence.split("\\|")) {
				String evidenceIri = String.format("<_:%s_evidence>", evidenceId);
				rdfWriter.write(String.format("%s %s %s .\n", assocInstanceId, BIOLINK_EVIDENCE, evidenceIri));
			}

		}
	}

	public static void main(String[] args) {
		File dir = new File(
				"/Users/bill/projects/ncats-translator/biolink-text-mining/relation-extraction/chebi-pr/chebi-pr-sentences/kgx");
		File kgxNodeFile = new File(dir, "text-mined.nodes.current.kgx.tsv");
		File kgxEdgeFile = new File(dir, "text-mined.edges.current.kgx.tsv");

		File outputFile = new File(dir, "chebi-pr.nt.gz");

		TrapiBackendSerializer backendSerializer = new AssocKgTrapiBackendSerializer();

		try (BufferedWriter writer = FileWriterUtil
				.initBufferedWriter(new GZIPOutputStream(new FileOutputStream(outputFile)), CharacterEncoding.UTF_8)) {

			backendSerializer.serializeNodes(kgxNodeFile, writer);
			backendSerializer.serializeEdges(kgxEdgeFile, writer);

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
