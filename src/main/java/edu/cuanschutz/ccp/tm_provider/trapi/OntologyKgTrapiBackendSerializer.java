package edu.cuanschutz.ccp.tm_provider.trapi;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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

public class OntologyKgTrapiBackendSerializer implements TrapiBackendSerializer {

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
			Set<String> biolinkCategories = CollectionsUtil.createSet(getFullBiolinkUri(category));
			String fullUri = getFullySpecifiedUri(id);

			/**
			 * <pre>
			 *  <http://purl.obolibrary.org/obo/CHEBI_3215> <https://w3id.org/biolink/vocab/id> "CHEBI:3215"@en .
			 * </pre>
			 */
			rdfWriter.write(String.format("%s %s \"%s\"@en .\n", fullUri, BIOLINK_ID, id));

			/**
			 * <pre>
			 * <http://purl.obolibrary.org/obo/CHEBI_3215> <http://www.w3.org/2000/01/rdf-schema#label> "bupivacaine"@en .
			 * </pre>
			 */
			rdfWriter.write(String.format("%s %s \"%s\"@en .\n", fullUri, RDFS_LABEL, label));

			/**
			 * <pre>
			 * <http://purl.obolibrary.org/obo/CHEBI_3215> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <https://w3id.org/biolink/vocab/ChemicalSubstance> .
			 * </pre>
			 */
			for (String biolinkCategory : biolinkCategories) {
				rdfWriter.write(String.format("%s %s %s .\n", fullUri, RDFS_SUBCLASSOF, biolinkCategory));
			}

		}

	}

	private String getFullySpecifiedUri(String id) {
		String prefix = id.split(":")[0];
		String suffix = id.split(":")[1];
		switch (prefix) {
		case "BFO":
			return String.format("%s%s>", OBO_PREFIX + "BFO_", suffix);
		case "CGNC":
			return String.format("<%s%s>", "http://birdgenenames.org/cgnc/", suffix);
		case "CHEBI":
			return String.format("%s%s>", OBO_PREFIX + "CHEBI_", suffix);
		case "CL":
			return String.format("%s%s>", OBO_PREFIX + "CL_", suffix);
		case "EcoGene":
			return String.format("%s%s>", OBO_PREFIX + "ECO_", suffix);
		case "Ensembl":
			return String.format("<%s%s>", "http://ensembl.org/id/", suffix);
		case "EnsemblGene":
			return String.format("<%s%s>", "http://ensembl.org/id/", suffix);
		case "EnsembleBacteria":
			return String.format("<%s%s>", "http://bacteria.ensembl.org/", suffix);
		case "FlyBase":
			return String.format("<%s%s>", "http://flybase.org/", suffix);
		case "GO":
			return String.format("%s%s>", OBO_PREFIX + "GO_", suffix);
		case "HGNC":
			return String.format("<%s%s>", "http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=", suffix);
		case "MOD":
			return String.format("%s%s>", OBO_PREFIX + "MOD_", suffix);
		case "NCBIGene":
			return String.format("<%s%s>", "http://www.ncbi.nlm.nih.gov/gene/", suffix);
		case "NCBITaxon":
			return String.format("%s%s>", OBO_PREFIX + "NCBITaxon_", suffix);
		case "OBI":
			return String.format("%s%s>", OBO_PREFIX + "OBI_", suffix);
		case "PR":
			return String.format("%s%s>", OBO_PREFIX + "PR_", suffix);
		case "PomBase":
			return String.format("<%s%s>", "https://www.pombase.org/spombe/result/", suffix);
		case "RGD":
			return String.format("<%s%s>", "http://rgd.mcw.edu/", suffix);
		case "SGD":
			return String.format("<%s%s>", "https://www.yeastgenome.org/locus/", suffix);
		case "SO":
			return String.format("%s%s>", OBO_PREFIX + "SO_", suffix);
		case "TAIR":
			return String.format("<%s%s>", "http://www.arabidopsis.org/", suffix);
		case "WormBase":
			return String.format("<%s%s>", "http://identifiers.org/wb/", suffix);
		case "ZFIN":
			return String.format("<%s%s>", "http://zfin.org/", suffix);
		case "dictyBase":
			return String.format("<%s%s>", "http://dictybase.org/gene/", suffix);
		case "UniProtKB":
			return String.format("<%s%s>", "http://purl.uniprot.org/uniprot/", suffix);
		case "MGI":
			return String.format("<%s%s>", "http://www.informatics.jax.org/MGI_", suffix);

		default:
			throw new IllegalArgumentException("Unhandled id type: " + id);
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

			String subjPurl = getFullySpecifiedUri(subjectId);
			String objPurl = getFullySpecifiedUri(objectId);
			String relationUri = getFullBiolinkUri(edgeLabel);

			/**
			 * <pre>
			 * </pre>
			 */
			rdfWriter.write(String.format("%s %s %s .\n", subjPurl, relationUri, objPurl));

		}
	}

	private String getFullBiolinkUri(String edgeLabel) {
		return "<" + edgeLabel.replace("biolink:", "https://w3id.org/biolink/vocab/") + ">";
	}

	public static void main(String[] args) {
		File dir = new File("/Users/bill/projects/ncats-translator/ontology-resources/ontologies/kgx");
		File kgxNodeFile = new File(dir, "pr.owl-subclass-hierarchy.nodes.kgx.tsv.gz");
		File kgxEdgeFile = new File(dir, "pr.owl-subclass-hierarchy.edges.kgx.tsv.gz");

		File outputFile = new File(dir, "pr-relations.nt.gz");

		TrapiBackendSerializer backendSerializer = new OntologyKgTrapiBackendSerializer();

		try (BufferedWriter writer = FileWriterUtil
				.initBufferedWriter(new GZIPOutputStream(new FileOutputStream(outputFile)), CharacterEncoding.UTF_8)) {

			backendSerializer.serializeNodes(kgxNodeFile, writer);
			backendSerializer.serializeEdges(kgxEdgeFile, writer);

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
