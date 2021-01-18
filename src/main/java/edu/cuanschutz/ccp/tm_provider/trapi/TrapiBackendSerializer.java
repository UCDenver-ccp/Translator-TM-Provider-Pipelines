package edu.cuanschutz.ccp.tm_provider.trapi;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;

/**
 * Interface for classes that process KGX files and create N-triples files that
 * will be loaded into the Blazegraph backend for the text-mining provider
 * TRAPIs
 *
 */
public interface TrapiBackendSerializer {

	public static final String OBO_PREFIX = "<http://purl.obolibrary.org/obo/";
	public static final String RDFS_LABEL = "<http://www.w3.org/2000/01/rdf-schema#label>";
	public static final String RDFS_SUBCLASSOF = "<http://www.w3.org/2000/01/rdf-schema#subClassOf>";
	public static final String RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ";
	public static final String BIOLINK_ID = "<https://w3id.org/biolink/vocab/id>";
	public static final String BIOLINK_NAME = "<https://w3id.org/biolink/vocab/name>";
	public static final String BIOLINK_PUBLICATIONS = "<https://w3id.org/biolink/vocab/publications>";
	public static final String BIOLINK_SENTENCE = "<https://w3id.org/biolink/vocab/sentence>";
	public static final String BIOLINK_SUBJECT_SPANS = "<https://w3id.org/biolink/vocab/subject_spans>";
	public static final String BIOLINK_OBJECT_SPANS = "<https://w3id.org/biolink/vocab/object_spans>";
	public static final String BIOLINK_PROVIDED_BY = "<https://w3id.org/biolink/vocab/provided_by>";
	public static final String BIOLINK_SCORE = "<https://w3id.org/biolink/vocab/score>";
	public static final String BIOLINK_SUBJECT = "<https://w3id.org/biolink/vocab/subject>";
	public static final String BIOLINK_OBJECT = "<https://w3id.org/biolink/vocab/object>";
	public static final String BIOLINK_RELATION = "<https://w3id.org/biolink/vocab/relation>";
	public static final String BIOLINK_EVIDENCE = "<https://w3id.org/biolink/vocab/evidence>";
	public static final String BIOLINK_CHEMICAL_SUBSTANCE = "<https://w3id.org/biolink/vocab/ChemicalSubstance>";
	public static final String BIOLINK_GENE_PRODUCT = "<https://w3id.org/biolink/vocab/GeneProduct>";
	public static final String BIOLINK_GENE_OR_GENE_PRODUCT = "<https://w3id.org/biolink/vocab/GeneOrGeneProduct>";
	public static final String BIOLINK_INFORMATION_CONTENT_ENTITY = "<https://w3id.org/biolink/vocab/InformationContentEntity>";
	public static final String BIOLINK_TEXT_MINED_EVIDENCE = "<https://w3id.org/biolink/vocab/TextMinedEvidence>";
	public static final String BIOLINK_ASSOCIATION = "<https://w3id.org/biolink/vocab/Association>";
	public static final String BIOLINK_CHEMICAL_TO_GENE_ASSOCIATION = "<https://w3id.org/biolink/vocab/ChemicalToGeneAssociation>";
	public static final String DIRECT_TYPE = "<http://www.openrdf.org/schema/sesame#directType>";
	public static final String RO_0002212 = "<http://purl.obolibrary.org/obo/RO_0002212>"; // negatively regulates
	public static final String RO_0002213 = "<http://purl.obolibrary.org/obo/RO_0002213>"; // positively regulates

	public void serializeNodes(File nodeFile, BufferedWriter rdfWriter) throws IOException;

	public void serializeEdges(File edgeFile, BufferedWriter rdfWriter) throws IOException;

}
