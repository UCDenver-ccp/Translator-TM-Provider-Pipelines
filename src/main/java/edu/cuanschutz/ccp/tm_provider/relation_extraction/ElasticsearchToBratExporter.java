package edu.cuanschutz.ccp.tm_provider.relation_extraction;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.annotations.VisibleForTesting;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ElasticsearchDocumentCreatorFn;
import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.BiolinkAssociation;
import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.BiolinkClass;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.digest.DigestUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileReaderUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil.FileSuffixEnforcement;
import edu.ucdenver.ccp.common.file.FileWriterUtil.WriteMode;
import edu.ucdenver.ccp.common.io.ClassPathUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;
import lombok.Data;

public class ElasticsearchToBratExporter {

	private static final CharacterEncoding UTF8 = CharacterEncoding.UTF_8;

	/**
	 * To avoid the annotator having to switch pages in BRAT after annotating each
	 * sentence, we will include multiple sentences on each page
	 */
	private static final int SENTENCES_PER_PAGE = 20;
	private static final int BATCH_SIZE = 500;

	protected static final String ELASTIC_BOOLEAN_QUERY_TEMPLATE = "elastic_boolean_query_template.json";
	private static final String ELASTIC_BOOLEAN_QUERY_TEMPLATE_MATCH_PLACEHOLDER = "MATCH_PLACEHOLDER";

	protected static final String ELASTIC_ANNOTATEDTEXT_MATCH_TEMPLATE = "elastic_annotatedtext_match_template.json";
	private static final String ELASTIC_ANNOTATEDTEXT_MATCH_TEMPLATE_QUERY_PLACEHOLDER = "QUERY_PLACEHOLDER";
	private static final String ELASTIC_ANNOTATEDTEXT_MATCH_TEMPLATE_BOOLEAN_OPERATOR_PLACEHOLDER = "BOOLEAN_OPERATOR_PLACEHOLDER";

	// @formatter:off
	static Set<String> IDENTIFIERS_TO_EXCLUDE = CollectionsUtil.createSet(
			"CHEBI:36080", 		// protein
			"PR:000000001", 	// protein
			"CL:0000000", 		// cell
			"MONDO:0000001", 	// disease
			"HP:0002664", 		// tumor
			"MONDO:0005070", 	// tumor
			"DRUGBANK:DB00118");
	// @formatter:on

	public static void createBratFiles(File outputDirectory, BiolinkAssociation biolinkAssociation, String batchId,
			Collection<? extends TextDocument> inputSentences, File previousSentenceIdsFile) throws IOException {

		createBratFiles(outputDirectory, biolinkAssociation, batchId, BATCH_SIZE, inputSentences,
				previousSentenceIdsFile, IDENTIFIERS_TO_EXCLUDE, SENTENCES_PER_PAGE);
	}

	/**
	 * @param biolinkAssociation
	 * @param batchId
	 * @param batchSize
	 * @param inputSentenceFiles
	 * @param previousSentenceIdsFile
	 * @param idsToExclude
	 * @param sentencesPerPage
	 * @throws IOException
	 */
	public static void createBratFiles(File outputDirectory, BiolinkAssociation biolinkAssociation, String batchId,
			int batchSize, Collection<? extends TextDocument> inputSentences, File previousSentenceIdsFile,
			Set<String> idsToExclude, int sentencesPerPage) throws IOException {

		Set<String> alreadyAnnotated = new HashSet<String>(
				FileReaderUtil.loadLinesFromFile(previousSentenceIdsFile, UTF8));

		int maxSentenceCount = inputSentences.size();

		System.out.println("Max sentence count: " + maxSentenceCount);

		Set<Integer> indexesForNewBatch = getRandomIndexes(maxSentenceCount, batchSize);

		System.out.println("random index count: " + indexesForNewBatch.size());

		Set<String> hashesOutputInThisBatch = new HashSet<String>();
		// this count is used to track when a batch has been completed
//		int extractedSentenceCount = 0;
		// this count is used to track the number of sentences that have been iterated
		// over -- this number is matched to the randomly generated indexes to select
		// sentence to include in the BRAT output
		int sentenceCount = 1;
//		String subBatchId = getSubBatchId(0);
		int subBatchIndex = 0;
		int spanOffset = 0;
		BufferedWriter annFileWriter = getAnnFileWriter(outputDirectory, biolinkAssociation, batchId, subBatchIndex);
		BufferedWriter txtFileWriter = getTxtFileWriter(outputDirectory, biolinkAssociation, batchId, subBatchIndex);
		// the idFile is not a BRAT file, but will be used to keep track of the
		// sentences that are being annotated and for any other metadata we might
		// need/want to store
		List<TextDocument> candidateSentences = new ArrayList<TextDocument>(inputSentences);
		try {
			int annIndex = 1;
			for (Integer index : indexesForNewBatch) {
				TextDocument td = candidateSentences.get(index);

				Set<BiolinkClass> biolinkClasses = new HashSet<BiolinkClass>(
						Arrays.asList(biolinkAssociation.getSubjectClass(), biolinkAssociation.getObjectClass()));

				td = excludeBasedOnEntityIds(td, idsToExclude, biolinkClasses);
				if (td != null) {
					String hash = computeHash(td);
					if (!alreadyAnnotated.contains(hash)) {
						if (hashesOutputInThisBatch.contains(hash)) {
							throw new IllegalStateException("duplicate hash observed!");
						}
						hashesOutputInThisBatch.add(hash);

						Indexes indexes = writeSentenceToBratFiles(td, new Indexes(spanOffset, annIndex), annFileWriter,
								txtFileWriter, biolinkAssociation);

						spanOffset = indexes.getSpanOffset();
						annIndex = indexes.getAnnIndex();

//						T index not incrementing properly
//						duplicate annotations end up in .ann files

						// create a new "page" of sentences at regular intervals by creating new
						// annFile, txtFile, and idFile.
						if (hashesOutputInThisBatch.size() % sentencesPerPage == 0
								&& hashesOutputInThisBatch.size() < batchSize) {
							// without this check for extractedSentenceCount < batchSize an empty file gets
							// created at the end of processing
							annFileWriter.close();
							txtFileWriter.write("DONE\n");
							txtFileWriter.close();

//						subBatchId = getSubBatchId(++subBatchIndex);

							annFileWriter = getAnnFileWriter(outputDirectory, biolinkAssociation, batchId,
									subBatchIndex);
							txtFileWriter = getTxtFileWriter(outputDirectory, biolinkAssociation, batchId,
									subBatchIndex);
							annIndex = 1;
							spanOffset = 0;
							subBatchIndex++;
						}
						if (hashesOutputInThisBatch.size() >= batchSize) {
							break;
						}
					}
				}
			}
		} finally {
			System.out.println("closing files (" + subBatchIndex + "). count = " + hashesOutputInThisBatch.size());
			annFileWriter.close();
			txtFileWriter.write("DONE\n");
			txtFileWriter.close();
		}

		System.out.println("Indexes for new batch count: " + indexesForNewBatch.size());
		System.out.println("Sentence count: " + sentenceCount);
		System.out.println("Hash output count: " + hashesOutputInThisBatch.size());

		/*
		 * save the hashes for sentences that were output during this batch to the file
		 * that tracks sentence hashes that have already been exported to a sheet for
		 * annotation
		 */
		try (BufferedWriter alreadyAnnotatedWriter = FileWriterUtil.initBufferedWriter(previousSentenceIdsFile, UTF8,
				WriteMode.APPEND, FileSuffixEnforcement.OFF)) {
			for (String hash : hashesOutputInThisBatch) {
				alreadyAnnotatedWriter.write(hash + "\n");
			}
		}

	}

	private static Indexes writeSentenceToBratFiles(TextDocument td, Indexes indexes, BufferedWriter annFileWriter,
			BufferedWriter txtFileWriter, BiolinkAssociation biolinkAssociation) throws IOException {

		txtFileWriter.write(td.getText() + "\n");

		List<TextAnnotation> annots = td.getAnnotations();
		Collections.sort(annots, TextAnnotation.BY_SPAN());

		int annIndex = indexes.getAnnIndex();
		int spanOffset = indexes.getSpanOffset();

		// use this set to prevent duplicate annotation lines from being written
		Set<String> alreadyWritten = new HashSet<String>();

		/* write the entity annotations to the ann file */
		for (TextAnnotation annot : annots) {
			String ontId = annot.getClassMention().getMentionName().toLowerCase();

			BiolinkClass biolinkClass = getBiolinkClassForOntologyId(biolinkAssociation, ontId);

			String spanStr = getSpanStr(annot, spanOffset);
			String coveredText = annot.getCoveredText();

			String annLineWithoutIndex = String.format("%s %s\t%s", biolinkClass.name().toLowerCase(), spanStr,
					coveredText);

			if (!alreadyWritten.contains(annLineWithoutIndex)) {
				String tIndex = "T" + annIndex++;
				String annLine = String.format("%s\t%s %s\t%s", tIndex, biolinkClass.name().toLowerCase(), spanStr,
						coveredText);
				alreadyWritten.add(annLineWithoutIndex);
				annFileWriter.write(annLine + "\n");
			}
		}

		spanOffset += td.getText().length() + 1;

		return new Indexes(spanOffset, annIndex);
	}

	@Data
	private static class Indexes {
		private final int spanOffset;
		private final int annIndex;
	}

	/**
	 * Given an ontology ID, return the BiolinkClass based on the ontology id prefix
	 * 
	 * @param biolinkAssociation
	 * @param ontId
	 * @return
	 */
	private static BiolinkClass getBiolinkClassForOntologyId(BiolinkAssociation biolinkAssociation, String ontId) {
		String ontPrefix = ontId.substring(0, ontId.indexOf(":")).toUpperCase();

		if (biolinkAssociation.getSubjectClass().getOntologyPrefixes().contains(ontPrefix)) {
			return biolinkAssociation.getSubjectClass();
		}

		if (biolinkAssociation.getObjectClass().getOntologyPrefixes().contains(ontPrefix)) {
			return biolinkAssociation.getObjectClass();
		}

		throw new IllegalArgumentException(
				String.format("Unable to map ontology prefix (%s) to a BiolinkClass from association: %s", ontPrefix,
						biolinkAssociation.name()));

	}

	private static String getSpanStr(TextAnnotation annot, int spanOffset) {
		List<Span> spans = annot.getSpans();
		Collections.sort(spans, Span.ASCENDING());
		StringBuilder sb = new StringBuilder();
		for (Span span : spans) {
			if (sb.length() > 0) {
				sb.append(";");
			}
			sb.append((span.getSpanStart() + spanOffset) + " " + (span.getSpanEnd() + spanOffset));
		}
		return sb.toString();
	}

//	/**
//	 * @param index
//	 * @return a 3-letter code in alpha order based on the input index number, e.g.
//	 *         0 = aaa, 1 = aab
//	 */
//	static String getSubBatchId(int index) {
//		char[] c = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
//				'u', 'v', 'w', 'x', 'y', 'z' };
//
//		// 0 = aaa 0,0,0
//		// 1 = aab 0,0,1
//		// 2 = aac 0,0,2
//
//		int index3 = index % 26;
//		int index2 = (index / 26) % 26;
//		int index1 = (index / (26 * 26)) % 26;
//
//		return "" + c[index1] + c[index2] + c[index3];
//	}

	/**
	 * @param td
	 * @param idsToExclude
	 * @param types        the Biolink classes that need to be in the sentence in
	 *                     order to be processed. If just one type is listed, then
	 *                     it is assumed that two annotations of that type are
	 *                     required.
	 * @return
	 */
	protected static TextDocument excludeBasedOnEntityIds(TextDocument td, Set<String> idsToExclude,
			Set<BiolinkClass> types) {

		Set<TextAnnotation> toKeep = new HashSet<TextAnnotation>();

		Map<BiolinkClass, Boolean> typeToPresenceMap = new HashMap<BiolinkClass, Boolean>();
		Map<String, BiolinkClass> ontologyPrefixToTypeMap = new HashMap<String, BiolinkClass>();
		for (BiolinkClass type : types) {
			typeToPresenceMap.put(type, false);
			for (String prefix : type.getOntologyPrefixes()) {
				ontologyPrefixToTypeMap.put(prefix, type);
			}
		}

		if (td.getAnnotations() != null) {
			for (TextAnnotation annot : td.getAnnotations()) {
				String conceptId = annot.getClassMention().getMentionName();
				String prefix = conceptId.substring(0, conceptId.indexOf(":"));
				if (!idsToExclude.contains(conceptId) && ontologyPrefixToTypeMap.containsKey(prefix)) {
					toKeep.add(annot);
					BiolinkClass biolinkClass = ontologyPrefixToTypeMap.get(prefix);
					typeToPresenceMap.put(biolinkClass, true);
				}
			}
		}

		HashSet<Boolean> presenceValues = new HashSet<Boolean>(typeToPresenceMap.values());
		if (presenceValues.size() == 1 && presenceValues.iterator().next() == true) {
			TextDocument toReturn = new TextDocument(td.getSourceid(), td.getSourcedb(), td.getText());
			toReturn.addAnnotations(toKeep);
			return toReturn;
		}

		return null;
	}

	protected static Set<Integer> getRandomIndexes(int maxSentenceCount, int batchSize) {
		Set<Integer> randomIndexes = new HashSet<Integer>();

		// add 100 extra just in case there are collisions with previous extracted
		// sentences
		Random rand = new Random();
		while (randomIndexes.size() < maxSentenceCount && randomIndexes.size() < batchSize + 10000) {
			randomIndexes.add(rand.nextInt(maxSentenceCount));
		}

		return randomIndexes;
	}

	private static BufferedWriter getTxtFileWriter(File outputDirectory, BiolinkAssociation biolinkAssociation,
			String batchId, int subBatchIndex) throws FileNotFoundException {
		return FileWriterUtil
				.initBufferedWriter(getTxtFile(outputDirectory, biolinkAssociation, batchId, subBatchIndex));
	}

	private static BufferedWriter getAnnFileWriter(File outputDirectory, BiolinkAssociation biolinkAssociation,
			String batchId, int subBatchIndex) throws FileNotFoundException {
		return FileWriterUtil
				.initBufferedWriter(getAnnFile(outputDirectory, biolinkAssociation, batchId, subBatchIndex));
	}

	static File getIdFile(File outputDirectory, BiolinkAssociation biolinkAssociation, String batchId,
			int subBatchIndex) {
		return new File(outputDirectory, getFilePrefix(biolinkAssociation, batchId, subBatchIndex) + ".id");
	}

	static File getTxtFile(File outputDirectory, BiolinkAssociation biolinkAssociation, String batchId,
			int subBatchIndex) {
		return new File(outputDirectory, getFilePrefix(biolinkAssociation, batchId, subBatchIndex) + ".txt");
	}

	static File getAnnFile(File outputDirectory, BiolinkAssociation biolinkAssociation, String batchId,
			int subBatchIndex) {
		return new File(outputDirectory, getFilePrefix(biolinkAssociation, batchId, subBatchIndex) + ".ann");
	}

	private static String getFilePrefix(BiolinkAssociation biolinkAssociation, String batchId, int subBatchIndex) {
		return String.format("%s_%s_%s", biolinkAssociation.name().toLowerCase(), batchId, subBatchIndex);
	}

	/**
	 * In this case the TextDocument is assumed to hold a single sentence + concept
	 * annotations
	 * 
	 * @param td
	 * @return
	 */
	public static String computeHash(TextDocument td) {
		return DigestUtil.getBase64Sha1Digest(td.getSourceid() + "_" + td.getText());
	}

	// authenitcation:
	// https://www.elastic.co/guide/en/elasticsearch/client/java-api-client/current/_other_authentication_methods.html
	public static Set<TextDocument> search(String elasticUrl, int elasticPort, String apiKeyAuth, String indexName,
			int maxReturned, Set<Set<String>> ontologyPrefixes) throws IOException {

		Header[] defaultHeaders = new Header[] { new BasicHeader("Authorization", "ApiKey " + apiKeyAuth) };
		Set<TextDocument> docsToReturn = new HashSet<TextDocument>();
		try (RestClient restClient = RestClient.builder(new HttpHost(elasticUrl, elasticPort, "https"))
				.setDefaultHeaders(defaultHeaders).build()) {

			// Create the transport with a Jackson mapper
			RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
			// And create the API client
			ElasticsearchClient client = new ElasticsearchClient(transport);
			BooleanResponse ping = client.ping();
//			System.out.println("HEALTH: " + client.cluster().health().clusterName());
			System.out.println("PING: " + ping.value());
//			System.out.println(info);
//			Query query = new Query.Builder()
//					.bool(_0 -> _0.must(_1 -> _1.term(t -> t                          
//					        .field("_index")     
//					        .value(v -> v.stringValue(indexName))
//					    )).must(_1 -> _1.term(t -> t                          
//					        .field("annotatedText")     
//				            .value(v -> v.stringValue("First")))
//					    )).build();  

//			Query query = new Query.Builder().match(t -> t.field("annotatedText").query("metformin")).build();
			String queryJson = buildSentenceQuery(ontologyPrefixes);
//			String queryJson = "{\n" + "    \"match\": {\n" + "        \"annotatedText\" : {\n"
//					+ "            \"query\": \"_DRUGBANK _GO\",\n" + "            \"operator\": \"and\"\n"
//					+ "        }\n" + "    }\n" + "  }";

			System.out.println("QUERY:\n" + queryJson);

			Query query = new Query.Builder().withJson(new ByteArrayInputStream(queryJson.getBytes())).build();

			SearchResponse<Sentence> results = client.search(_0 -> _0.size(maxReturned).index(indexName).query(query),
					Sentence.class);

			Set<String> ontologyPrefixesToIncludeInSearchHits = CollectionsUtil.consolidateSets(ontologyPrefixes);
			for (Hit<Sentence> hit : results.hits().hits()) {
				TextDocument td = deserializeAnnotatedText(hit.source().getDocumentId(),
						hit.source().getAnnotatedText(), ontologyPrefixesToIncludeInSearchHits);
				docsToReturn.add(td);
			}
		}
		return docsToReturn;

	}

	/**
	 * 
	 * @param ontologyPrefixes
	 * @return an elasticsearch query json that requires the presence of all
	 *         ontology prefix sets in order to match
	 * @throws IOException
	 */
	@VisibleForTesting
	protected static String buildSentenceQuery(Set<Set<String>> ontologyPrefixes) throws IOException {
		// load boolean query template
		String booleanQueryTemplate = ClassPathUtil.getContentsFromClasspathResource(ElasticsearchToBratExporter.class,
				ELASTIC_BOOLEAN_QUERY_TEMPLATE, UTF8);

		// load annotatedText match template
		String annotatedTextMatchTemplate = ClassPathUtil.getContentsFromClasspathResource(
				ElasticsearchToBratExporter.class, ELASTIC_ANNOTATEDTEXT_MATCH_TEMPLATE, UTF8);

		// sorting is required only for unit tests so that the output order is
		// deterministic
		List<String> sortedOntologyPrefixQueryStrings = getSortedOntologyPrefixQueryStrings(ontologyPrefixes);

		StringBuilder matchStanzas = new StringBuilder();
		for (String ontologyPrefixQueryString : sortedOntologyPrefixQueryStrings) {
			if (matchStanzas.length() > 0) {
				matchStanzas.append(",\n");
			}
			String matchStanza = createAnnotatedTextMatchStanza(annotatedTextMatchTemplate, ontologyPrefixQueryString);
			matchStanzas.append(matchStanza);
		}

		matchStanzas.append("\n");
		String matches = matchStanzas.toString();
		String query = booleanQueryTemplate.replace(ELASTIC_BOOLEAN_QUERY_TEMPLATE_MATCH_PLACEHOLDER, matches);

		return query;
	}

	/**
	 * This method adds an underscore to the beginning of each ontology prefix (to
	 * match how they are represented in the Elastic index)
	 * 
	 * @param ontologyPrefixSets
	 * @return
	 */
	private static List<String> getSortedOntologyPrefixQueryStrings(Set<Set<String>> ontologyPrefixSets) {
		List<String> ontologyPrefixQueryStrings = new ArrayList<String>();
		for (Set<String> ontologyPrefixSet : ontologyPrefixSets) {
			List<String> sortedPrefixes = new ArrayList<String>(ontologyPrefixSet);
			Collections.sort(sortedPrefixes);
			StringBuilder sb = new StringBuilder();
			for (String prefix : sortedPrefixes) {
				sb.append("_" + prefix + " ");
			}
			ontologyPrefixQueryStrings.add(sb.toString().trim());
		}
		Collections.sort(ontologyPrefixQueryStrings);
		return ontologyPrefixQueryStrings;
	}

	/**
	 * @param annotatedTextMatchTemplate
	 * @param ontologyPrefixSet
	 * @return a populated elasticsearch match block that or's together the input
	 *         ontology prefixes
	 */
	@VisibleForTesting
	protected static String createAnnotatedTextMatchStanza(String annotatedTextMatchTemplate,
			String ontologyPrefixQueryString) {
		String matchStanza = annotatedTextMatchTemplate;
		matchStanza = matchStanza.replace(ELASTIC_ANNOTATEDTEXT_MATCH_TEMPLATE_QUERY_PLACEHOLDER,
				ontologyPrefixQueryString);
		matchStanza = matchStanza.replace(ELASTIC_ANNOTATEDTEXT_MATCH_TEMPLATE_BOOLEAN_OPERATOR_PLACEHOLDER, "or");
		return matchStanza;
	}

	/**
	 * @param documentId
	 * @param annotatedText
	 * @param ontologyPrefixes only keep annotations with the specified ontology
	 *                         prefixes
	 * @return
	 */
	@VisibleForTesting
	protected static TextDocument deserializeAnnotatedText(String documentId, String annotatedText,
			Set<String> ontologyPrefixes) {

		String decodedAnnotatedText = decode(annotatedText);

		Pattern p = Pattern.compile("\\(([^\\(]*?)\\)\\[(.*?)\\]");
		Matcher m = p.matcher(decodedAnnotatedText);

		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();
		List<TextAnnotation> annots = new ArrayList<TextAnnotation>();
		StringBuilder sb = new StringBuilder();
		int annotatedTextOffset = 0;
		int sentenceTextOffset = 0;
		while (m.find()) {
			String interveningText = decodedAnnotatedText.substring(annotatedTextOffset, m.start());
			sb.append(interveningText);
			String coveredText = m.group(1);
			sb.append(coveredText);

			int spanStart = m.start() - sentenceTextOffset;
			int spanEnd = spanStart + coveredText.length();

			String[] conceptIds = m.group(2).split("&");

			for (String conceptId : conceptIds) {
				if (!conceptId.startsWith("_")) {
					String ontologyPrefix = conceptId.substring(0, conceptId.indexOf("_"));
					if (ontologyPrefixes.contains(ontologyPrefix)) {
						TextAnnotation annotation = factory.createAnnotation(spanStart, spanEnd, coveredText,
								conceptId.replace("_", ":"));
						annots.add(annotation);
					}
				}
			}

			annotatedTextOffset = m.end();
			sentenceTextOffset += (m.group().length() - coveredText.length());

		}

		sb.append(decodedAnnotatedText.substring(annotatedTextOffset));

		TextDocument td = new TextDocument(documentId, "PubMed", sb.toString());
		td.addAnnotations(annots);

		return td;
	}

	/**
	 * @param annotatedText
	 * @return the annotated text decoded so that characters that were encoded for
	 *         HTTP serialization purposes are converted back to their un-encoded
	 *         form, e.g parentheses.
	 */
	private static String decode(String annotatedText) {
		return ElasticsearchDocumentCreatorFn.decode(annotatedText);
	}

	@Data
	@JsonIgnoreProperties(ignoreUnknown = true)
	private static class Sentence {

		@SuppressWarnings("unused")
		public static final String ID = "id";
		@SuppressWarnings("unused")
		public static final String DOCUMENT_ID = "documentId";
		@SuppressWarnings("unused")
		public static final String ANNOTATED_TEXT = "annotatedText";

		private String id;
		private String documentId;
		private String annotatedText;

	}

}
