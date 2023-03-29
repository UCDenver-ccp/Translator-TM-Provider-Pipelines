package edu.cuanschutz.ccp.tm_provider.relation_extraction.distant_supervision;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;

import com.google.common.annotations.VisibleForTesting;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ElasticsearchDocumentCreatorFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ElasticsearchDocumentCreatorFn.Sentence;
import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.BiolinkAssociation;
import edu.cuanschutz.ccp.tm_provider.relation_extraction.ElasticsearchToBratExporter;
import edu.cuanschutz.ccp.tm_provider.relation_extraction.distant_supervision.ConceptPairsFileParser.ConceptPair;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.reader.Line;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import edu.ucdenver.ccp.common.io.ClassPathUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;

public class ConceptPairSentenceExtractor {

	private static final CharacterEncoding UTF8 = CharacterEncoding.UTF_8;

	protected static final String ELASTIC_BOOLEAN_QUERY_TEMPLATE = "elastic_boolean_query_template.json";
	private static final String ELASTIC_BOOLEAN_QUERY_TEMPLATE_MATCH_PLACEHOLDER = "MATCH_PLACEHOLDER";

	protected static final String ELASTIC_ANNOTATEDTEXT_MATCH_TEMPLATE = "elastic_annotatedtext_match_template.json";
	private static final String ELASTIC_ANNOTATEDTEXT_MATCH_TEMPLATE_QUERY_PLACEHOLDER = "QUERY_PLACEHOLDER";
	private static final String ELASTIC_ANNOTATEDTEXT_MATCH_TEMPLATE_BOOLEAN_OPERATOR_PLACEHOLDER = "BOOLEAN_OPERATOR_PLACEHOLDER";

	// these identifiers are useful when searching for "negative" examples; we don't
	// want such general concepts
	// @formatter:off
	static Set<String> IDENTIFIERS_TO_EXCLUDE = CollectionsUtil.createSet(
			"CHEBI:36080", 		// protein
			"PR:000000001", 	// protein
			"CL:0000000", 		// cell
			"MONDO:0000001", 	// disease
			"HP:0002664", 		// tumor
			"MONDO:0005070", 	// tumor
			"DRUGBANK:DB00118", //
			"GO:0005575", // cellular_component
			"GO:0110165", // cellular anatomical entity
			"GO:0032991", // protein-containing complex
			"GO:0044423", // virion component
			"GO:0005622"  // intracellular anatomical structure
			
			);
	// @formatter:on

	public static void search(String elasticUrl, int elasticPort, String apiKeyAuth, String indexName, int maxReturned,
			Set<ConceptPair> conceptPairs, BufferedWriter writer, BiolinkAssociation association, Set<String> conceptIdsToExclude) throws IOException {

		Map<ConceptPair, Set<TextDocument>> map = new HashMap<ConceptPair, Set<TextDocument>>();

		Header[] defaultHeaders = new Header[] { new BasicHeader("Authorization", "ApiKey " + apiKeyAuth) };

		try (RestClient restClient = RestClient.builder(new HttpHost(elasticUrl, elasticPort, "https"))
				.setDefaultHeaders(defaultHeaders).build()) {

			RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
			ElasticsearchClient client = new ElasticsearchClient(transport);
			BooleanResponse ping = client.ping();
			System.out.println("PING: " + ping.value());

			int sentenceCount = 0;
			int pairCount = 0;
			for (ConceptPair conceptPair : conceptPairs) {
				try {
//				System.out.println("Searching for negative concept pair: " + conceptPair.toString());
					if (pairCount++ % 100 == 0) {
						System.out.println(String.format("Progress: %d of %d | sentences: %d", (pairCount - 1),
								conceptPairs.size(), sentenceCount));
					}

					Set<TextDocument> docsToReturn = new HashSet<TextDocument>();

					String queryJson = buildConceptPairQuery(conceptPair);
//				System.out.println("QUERY:\n" + queryJson);

					Query query = new Query.Builder().withJson(new ByteArrayInputStream(queryJson.getBytes())).build();
					SearchRequest request = SearchRequest.of(_0 -> _0.size(maxReturned).index(indexName).query(query));

					SearchResponse<Sentence> response = client.search(request, Sentence.class);
					String scrollId = response.scrollId();
					List<Hit<Sentence>> hits = response.hits().hits();
//				System.out.println("HITS: " + hits.size());
					for (Hit<Sentence> hit : hits) {
						sentenceCount++;
						TextDocument td = deserializeAnnotatedText(hit.source().getDocumentId(),
								hit.source().getAnnotatedText());
						docsToReturn.add(td);
					}

					ConceptPairsSentenceExtractorMain.writeMaskedSentences(association, conceptPair, docsToReturn,
							writer, conceptIdsToExclude);
//				map.put(conceptPair, docsToReturn);

				} catch (IOException e) {
					System.err.println(
							"Caught IOException for pair: " + conceptPair.toString() + " -- " + e.getMessage());
				}
				if (sentenceCount > 50000) {
					break;
				}
			}
		}
//		return map;

	}

//	public static Map<ConceptPair, Set<TextDocument>> search(String elasticUrl, int elasticPort, String apiKeyAuth,
//			String indexName, int maxReturned, Map<String, Set<String>> subjectToObjectCuriesMap) throws IOException {
//
//		Map<ConceptPair, Set<TextDocument>> map = new HashMap<ConceptPair, Set<TextDocument>>();
//
//		Header[] defaultHeaders = new Header[] { new BasicHeader("Authorization", "ApiKey " + apiKeyAuth) };
//
//		try (RestClient restClient = RestClient.builder(new HttpHost(elasticUrl, elasticPort, "https"))
//				.setDefaultHeaders(defaultHeaders).build()) {
//
//			RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
//			ElasticsearchClient client = new ElasticsearchClient(transport);
//			BooleanResponse ping = client.ping();
//			System.out.println("PING: " + ping.value());
//
//			for (Entry<String, Set<String>> entry : subjectToObjectCuriesMap.entrySet()) {
//				Set<TextDocument> docsToReturn = new HashSet<TextDocument>();
//
//				String queryJson = buildConceptPairQuery(entry);
//				System.out.println("QUERY:\n" + queryJson);
//
//				Query query = new Query.Builder().withJson(new ByteArrayInputStream(queryJson.getBytes())).build();
//				SearchRequest request = SearchRequest.of(_0 -> _0.size(maxReturned).index(indexName).query(query));
//
//				SearchResponse<Sentence> response = client.search(request, Sentence.class);
//				String scrollId = response.scrollId();
//				List<Hit<Sentence>> hits = response.hits().hits();
//				System.out.println("HITS: " + hits.size());
//				for (Hit<Sentence> hit : hits) {
//					TextDocument td = deserializeAnnotatedText(hit.source().getDocumentId(),
//							hit.source().getAnnotatedText());
//					docsToReturn.add(td);
//				}
//
//				map.put(conceptPair, docsToReturn);
//
//			}
//		}
//		return map;
//
//	}

	@VisibleForTesting
	protected static TextDocument deserializeAnnotatedText(String documentId, String annotatedText) {

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
					try {
						String id = conceptId.replace("_", ":");
						TextAnnotation annotation = factory.createAnnotation(spanStart, spanEnd, coveredText, id);
						annots.add(annotation);
					} catch (Exception e) {
						System.err.println("NO UNDERSCORE: " + conceptId);
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

	/**
	 * 
	 * @param ontologyPrefixes
	 * @return an elasticsearch query json that requires the presence of all
	 *         ontology prefix sets in order to match
	 * @throws IOException
	 */
	@VisibleForTesting
	protected static String buildConceptPairQuery(ConceptPair conceptPair) throws IOException {
		// load boolean query template
		String booleanQueryTemplate = ClassPathUtil.getContentsFromClasspathResource(ElasticsearchToBratExporter.class,
				ELASTIC_BOOLEAN_QUERY_TEMPLATE, UTF8);

		// load annotatedText match template
		String annotatedTextMatchTemplate = ClassPathUtil.getContentsFromClasspathResource(
				ElasticsearchToBratExporter.class, ELASTIC_ANNOTATEDTEXT_MATCH_TEMPLATE, UTF8);

		// sorting is required only for unit tests so that the output order is
		// deterministic
		List<String> sortedConceptCuries = getSortedCuries(conceptPair);

		StringBuilder matchStanzas = new StringBuilder();
		for (String conceptCurieQueryString : sortedConceptCuries) {
			if (matchStanzas.length() > 0) {
				matchStanzas.append(",\n");
			}
			String matchStanza = createAnnotatedTextMatchStanza(annotatedTextMatchTemplate, conceptCurieQueryString);
			matchStanzas.append(matchStanza);
		}

		matchStanzas.append("\n");
		String matches = matchStanzas.toString();
		String query = booleanQueryTemplate.replace(ELASTIC_BOOLEAN_QUERY_TEMPLATE_MATCH_PLACEHOLDER, matches);

		return query;
	}

//	/**
//	 * the query must match the subject curie with any of the object curies
//	 * @param subjectToObjectCurieEntry
//	 * @return
//	 * @throws IOException
//	 */
//	protected static String buildConceptPairQuery(Entry<String, Set<String>> subjectToObjectCurieEntry) throws IOException {
//		// load boolean query template
//		String booleanQueryTemplate = ClassPathUtil.getContentsFromClasspathResource(ElasticsearchToBratExporter.class,
//				ELASTIC_BOOLEAN_QUERY_TEMPLATE, UTF8);
//
//		// load annotatedText match template
//		String annotatedTextMatchTemplate = ClassPathUtil.getContentsFromClasspathResource(
//				ElasticsearchToBratExporter.class, ELASTIC_ANNOTATEDTEXT_MATCH_TEMPLATE, UTF8);
//
//		// sorting is required only for unit tests so that the output order is
//		// deterministic
//		List<String> sortedConceptCuries = getSortedCuries(subjectToObjectCurieEntry);
//
//		StringBuilder matchStanzas = new StringBuilder();
//		for (String conceptCurieQueryString : sortedConceptCuries) {
//			if (matchStanzas.length() > 0) {
//				matchStanzas.append(",\n");
//			}
//			String matchStanza = createAnnotatedTextMatchStanza(annotatedTextMatchTemplate, conceptCurieQueryString);
//			matchStanzas.append(matchStanza);
//		}
//
//		matchStanzas.append("\n");
//		String matches = matchStanzas.toString();
//		String query = booleanQueryTemplate.replace(ELASTIC_BOOLEAN_QUERY_TEMPLATE_MATCH_PLACEHOLDER, matches);
//
//		return query;
//	}

//	private static List<String> getSortedCuries(Entry<String, Set<String>> subjectToObjectCurieEntry) {
//		List<String> list = new ArrayList<String>();
//
//		List<String> objectCuries = new ArrayList<String>();
//		for (String obj : subjectToObjectCurieEntry.getValue()) {
//			objectCuries.add(obj.replace(":", "_"));
//		}
//		Collections.sort(objectCuries);
//		
//		list.add(CollectionsUtil.createDelimitedString(objectCuries, " "));
//		list.add(subjectToObjectCurieEntry.getKey().replace(":", "_"));
//
//		Collections.sort(list);
//		return list;
//	}

	private static List<String> getSortedCuries(ConceptPair conceptPair) {
		List<String> list = new ArrayList<String>();

		list.add(conceptPair.getSubjectCurie().replace(":", "_"));

		List<String> objectCuries = new ArrayList<String>();
		for (String obj : conceptPair.getObjectCuries()) {
			objectCuries.add(obj.replace(":", "_"));
		}
		Collections.sort(objectCuries);
		list.add(CollectionsUtil.createDelimitedString(objectCuries, " "));

		Collections.sort(list);
		return list;
	}

	/**
	 * @param annotatedTextMatchTemplate
	 * @param ontologyPrefixSet
	 * @return a populated elasticsearch match block that or's together the input
	 *         ontology prefixes
	 * @throws IOException
	 */
	public static String createAnnotatedTextMatchStanza(String annotatedTextMatchTemplate,
			String ontologyPrefixQueryString) throws IOException {
		String matchStanza = annotatedTextMatchTemplate;
		matchStanza = matchStanza.replace(ELASTIC_ANNOTATEDTEXT_MATCH_TEMPLATE_QUERY_PLACEHOLDER,
				ontologyPrefixQueryString);
		matchStanza = matchStanza.replace(ELASTIC_ANNOTATEDTEXT_MATCH_TEMPLATE_BOOLEAN_OPERATOR_PLACEHOLDER, "or");

		if (!ontologyPrefixQueryString.contains(" ")) {
			// then we need to remove the operator line which is the 5th line
			StringBuilder sb = new StringBuilder();
			for (StreamLineIterator lineIter = new StreamLineIterator(new ByteArrayInputStream(matchStanza.getBytes()),
					UTF8, null); lineIter.hasNext();) {
				Line line = lineIter.next();
				String lineText = line.getText();
				if (lineText.contains("query")) {
					// remove trailing comma
					lineText = lineText.substring(0, lineText.length() - 1);
				}
				if (!lineText.contains("operator")) {
					sb.append(lineText + "\n");
				}
			}
			matchStanza = sb.toString();
		}

		return matchStanza;
	}
}
