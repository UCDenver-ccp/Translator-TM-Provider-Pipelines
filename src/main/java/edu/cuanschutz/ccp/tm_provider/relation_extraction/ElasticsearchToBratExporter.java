package edu.cuanschutz.ccp.tm_provider.relation_extraction;

import static edu.cuanschutz.ccp.tm_provider.etl.fn.ElasticsearchDocumentCreatorFn.computeSentenceIdentifier;

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
import java.util.Map.Entry;
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
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.ScrollRequest;
import co.elastic.clients.elasticsearch.core.ScrollResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ElasticsearchDocumentCreatorFn;
import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.BiolinkAssociation;
import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.BiolinkClass;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.collections.CollectionsUtil.SortOrder;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.reader.Line;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import edu.ucdenver.ccp.common.io.ClassPathUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;
import lombok.Data;

public class ElasticsearchToBratExporter {

	private static final CharacterEncoding UTF8 = CharacterEncoding.UTF_8;

	/**
	 * SEARCH_BATCH_SIZE is the number of records requested for each Elasticsearch
	 * query
	 */
	private static final int SEARCH_BATCH_SIZE = 10000;

	public static final String ELASTIC_BOOLEAN_QUERY_TEMPLATE = "elastic/elastic_boolean_query_template.json";
	public static final String ELASTIC_BOOLEAN_QUERY_TEMPLATE_MATCH_PLACEHOLDER = "MATCH_PLACEHOLDER";

	public static final String ELASTIC_ANNOTATEDTEXT_MATCH_TEMPLATE = "elastic/elastic_annotatedtext_match_template.json";
	public static final String ELASTIC_ANNOTATEDTEXT_MATCH_TEMPLATE_QUERY_PLACEHOLDER = "QUERY_PLACEHOLDER";
	public static final String ELASTIC_ANNOTATEDTEXT_MATCH_TEMPLATE_BOOLEAN_OPERATOR_PLACEHOLDER = "BOOLEAN_OPERATOR_PLACEHOLDER";

	// @formatter:off
	public static Set<String> IDENTIFIERS_TO_EXCLUDE = CollectionsUtil.createSet(
			"CHEBI:36080", 		// protein
			"PR:000000001", 	// protein
			"CL:0000000", 		// cell
			"MONDO:0000001", 	// disease
			"HP:0002664", 		// tumor
			"MONDO:0005070", 	// tumor
			"DRUGBANK:DB00118");
	// @formatter:on

	/**
	 * @param outputDirectory
	 * @param biolinkAssociation
	 * @param batchId
	 * @param sentencesPerPage            determines how many sentences in each BRAT
	 *                                    file, i.e., how many will show up on a
	 *                                    single page in the BRAT UI
	 * @param inputSentences
	 * @param previousSentenceIdsFile
	 * @param redundantSentencesToInclude
	 * @throws IOException
	 */
	public static void createBratFiles(File outputDirectory, BiolinkAssociation biolinkAssociation, String batchId,
			int batchSize, int sentencesPerPage, Collection<? extends TextDocument> inputSentences,
			Set<String> previousSentenceIds, List<TextDocument> redundantSentencesToInclude) throws IOException {

		createBratFiles(outputDirectory, biolinkAssociation, batchId, batchSize, inputSentences, previousSentenceIds,
				IDENTIFIERS_TO_EXCLUDE, sentencesPerPage, redundantSentencesToInclude);
	}

	/**
	 * Creates n batch files, 1 per annotator. N is specified by the number of
	 * output directories submitted.
	 * 
	 * 
	 * 
	 * @param biolinkAssociation
	 * @param batchId
	 * @param batchSize
	 * @param inputSentenceFiles
	 * @param previousSentenceIdsFile
	 * @param entityOntologyIdsToExclude
	 * @param sentencesPerPage
	 * @param redundantSentencesToInclude - these are sentences from a different
	 *                                    annotator batch that will be included in
	 *                                    this batch so that inter-annotator
	 *                                    agreement can be calculated. They will be
	 *                                    randomly worked into the randomly selected
	 *                                    sentences for this batch.
	 * @param associationDir
	 * @param batchOverlapPercentage
	 * @throws IOException
	 */
	public static Set<String> createBratFiles(File outputDirectory, BiolinkAssociation biolinkAssociation,
			String batchId, int batchSize, Collection<? extends TextDocument> inputSentences,
			Set<String> alreadyAnnotatedSentenceIds, Set<String> entityOntologyIdsToExclude, int sentencesPerPage,
			List<TextDocument> redundantSentencesToInclude) throws IOException {

		for (TextDocument td : redundantSentencesToInclude) {
			if (td.getAnnotations() == null) {
				System.err.println("NULL ANNOTS IN REDUNDANT DOC");
			}
		}

		int maxSentenceCount = inputSentences.size();

//		System.out.println("Max sentence count: " + maxSentenceCount);

		List<Integer> indexesForNewBatch = getRandomIndexes(maxSentenceCount, batchSize,
				redundantSentencesToInclude.size());

//		System.out.println("random index count: " + indexesForNewBatch.size());
//		System.out.println("random indexes: " + indexesForNewBatch.toString());

		Set<String> hashesOutputInThisBatch = new HashSet<String>();
		// this count is used to track when a batch has been completed
//		int extractedSentenceCount = 0;
		// this count is used to track the number of sentences that have been iterated
		// over -- this number is matched to the randomly generated indexes to select
		// sentence to include in the BRAT output
		int sentenceCount = 0;
//		String subBatchId = getSubBatchId(0);
		int subBatchIndex = 0;
		int spanOffset = 0;

		BufferedWriter annFileWriter = getAnnFileWriter(outputDirectory, biolinkAssociation, batchId, subBatchIndex);
		BufferedWriter txtFileWriter = getTxtFileWriter(outputDirectory, biolinkAssociation, batchId, subBatchIndex);

		List<TextDocument> candidateSentences = new ArrayList<TextDocument>(inputSentences);
//		System.out.println("INDEXES FOR NEW BATCH: " + indexesForNewBatch.size());
		try {
			int redundantIndex = 0;
			int annIndex = 1;
			for (Integer index : indexesForNewBatch) {
				boolean checkHash = true;
				TextDocument td = null;
				if (index == -1) {
					// then get one of the redundant sentences that will eventually be used to
					// compute inter-annotator agreement
					td = redundantSentencesToInclude.get(redundantIndex++);
					checkHash = false;
				} else {
					// get a randomly sampled sentence
					td = candidateSentences.get(index);
					Set<BiolinkClass> biolinkClasses = new HashSet<BiolinkClass>(
							Arrays.asList(biolinkAssociation.getSubjectClass(), biolinkAssociation.getObjectClass()));
					td = excludeBasedOnEntityIds(td, entityOntologyIdsToExclude, biolinkClasses);
				}

				if (td.getAnnotations() == null) {
					// this error persists even after screening the docs as they come back from
					// elasticsearch - so we must be inadvertantly deleting annotations at some
					// point? strange.
					System.err.println("Null annotations observed!!!!");
					td = null;
				}

				if (td != null) {
					String hash = computeSentenceIdentifier(td.getText());
					// for the redundant sentences, we don't want to check the hash (checkHash ==
					// false) b/c it will be in the alreadyAnnotated set, but we need to add the
					// hash to the hashesOutputInThisBatch so that the correct number of sentences
					// get output per file.
					if (!hashesOutputInThisBatch.contains(hash)
							&& (checkHash == false || !alreadyAnnotatedSentenceIds.contains(hash))) {
						if (hashesOutputInThisBatch.contains(hash)) {
							throw new IllegalStateException("duplicate hash observed!");
						}
						hashesOutputInThisBatch.add(hash);

						Indexes indexes = writeSentenceToBratFiles(td, new Indexes(spanOffset, annIndex), annFileWriter,
								txtFileWriter, biolinkAssociation);
						sentenceCount++;
						spanOffset = indexes.getSpanOffset();
						annIndex = indexes.getAnnIndex();

						// create a new "page" of sentences at regular intervals by creating new
						// annFile, txtFile, and idFile.
						if (hashesOutputInThisBatch.size() % sentencesPerPage == 0
								&& hashesOutputInThisBatch.size() < batchSize) {
							// without this check for extractedSentenceCount < batchSize an empty file gets
							// created at the end of processing
							annFileWriter.close();
							txtFileWriter.write("DONE\n");
							txtFileWriter.close();

							subBatchIndex++;
							annFileWriter = getAnnFileWriter(outputDirectory, biolinkAssociation, batchId,
									subBatchIndex);
							txtFileWriter = getTxtFileWriter(outputDirectory, biolinkAssociation, batchId,
									subBatchIndex);
							annIndex = 1;
							spanOffset = 0;
						}
						if (hashesOutputInThisBatch.size() >= batchSize) {
//							System.out.println("BREAKING: HASH SIZE = BATCH SIZE");
							break;
						}
					}
				}
			}

//			System.out.println("LOOP ENDED: HASHES OUTPUT: " + hashesOutputInThisBatch.size());
//			System.out.println("LOOP ENDED: BATCH SIZE: " + batchSize);
		} finally {

//			System.out.println("closing files (" + subBatchIndex + "). count = " + hashesOutputInThisBatch.size());
			annFileWriter.close();
			txtFileWriter.write("DONE\n");
			txtFileWriter.close();
		}

//		System.out.println("Indexes for new batch count: " + indexesForNewBatch.size());
		System.out.println(
				"Batch creation complete. Sentences included in this batch: " + hashesOutputInThisBatch.size());
//		System.out.println("Hash output count: " + hashesOutputInThisBatch.size());

		return hashesOutputInThisBatch;

	}

	private static Indexes writeSentenceToBratFiles(TextDocument td, Indexes indexes, BufferedWriter annFileWriter,
			BufferedWriter txtFileWriter, BiolinkAssociation biolinkAssociation) throws IOException {

		txtFileWriter.write(td.getText() + "\n");

		List<TextAnnotation> annots = td.getAnnotations();
		if (annots == null) {
			throw new IllegalStateException("Null annots for: " + td.toString());
		}

		Collections.sort(annots, TextAnnotation.BY_SPAN());

		int annIndex = indexes.getAnnIndex();
		int spanOffset = indexes.getSpanOffset();

		// use this set to prevent duplicate annotation lines from being written
		Set<String> alreadyWritten = new HashSet<String>();

		/* write the entity annotations to the ann file */
		for (TextAnnotation annot : annots) {
			String ontId = annot.getClassMention().getMentionName().toLowerCase();

			BiolinkClass biolinkClass = getBiolinkClassForOntologyId(biolinkAssociation, ontId);
			if (biolinkClass != null) {

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

		if (!ontId.contains(":")) {
			// we assume that it is already a biolink class
			try {
				return BiolinkClass.valueOf(ontId.toUpperCase());
			} catch (IllegalArgumentException e) {
				System.err.println("Encountered non-biolink class: " + ontId);
				return null;
			}
		}

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

	/**
	 * @param td
	 * @param entityOntologyIdsToExclude
	 * @param types                      the Biolink classes that need to be in the
	 *                                   sentence in order to be processed. If just
	 *                                   one type is listed, then it is assumed that
	 *                                   two annotations of that type are required.
	 * @return
	 */
	protected static TextDocument excludeBasedOnEntityIds(TextDocument td, Set<String> entityOntologyIdsToExclude,
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
				if (!entityOntologyIdsToExclude.contains(conceptId) && ontologyPrefixToTypeMap.containsKey(prefix)) {
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

	protected static List<Integer> getRandomIndexes(int maxSentenceCount, int batchSize,
			int redundantSentencesToIncludeCount) {

//		System.out.println("CREATING RANDOM INDEXES. BATCH SIZE: " + batchSize);
//		System.out.println("CREATING RANDOM INDEXES. MAX SENTENCE COUNT: " + maxSentenceCount);
//		System.out.println("CREATING RANDOM INDEXES. REDUNDANT TO INCLUDE: " + redundantSentencesToIncludeCount);

		if (batchSize < redundantSentencesToIncludeCount) {
			throw new IllegalArgumentException(String.format(
					"Batch size (%d) is less than the count of redundant sentences to include (%d). "
							+ "This cannot be as some of the redundant sentences would be excluded. Please increase batch size.",
					batchSize, redundantSentencesToIncludeCount));
		}

		Set<Integer> randomIndexes = new HashSet<Integer>();

		Random rand = new Random();
		// by subtracting redundantSentencesToIncludeCount we are leaving room to add
		// the redundant sentences
		while (randomIndexes.size() < (maxSentenceCount - redundantSentencesToIncludeCount)
				&& randomIndexes.size() < (batchSize - redundantSentencesToIncludeCount)) {
			randomIndexes.add(rand.nextInt(maxSentenceCount));
		}

		List<Integer> randomIndexesList = new ArrayList<Integer>(randomIndexes);

		// the redundant sentences need to make it into the batch, so their indexes need
		// to be in the 1st n (where n is the batch size) of the random indexes that get
		// returned from this method. We will use -1 as the placeholder for the
		// redundant sentence index.

		// get n random indexes between 0 and batchSize where n =
		// redundantSentencesToIncludeCount
		Set<Integer> redundantIndexes = new HashSet<Integer>();
		rand = new Random();
		while (redundantIndexes.size() < redundantSentencesToIncludeCount) {
			redundantIndexes.add(rand.nextInt(batchSize));
		}
		List<Integer> redundantIndexesList = new ArrayList<Integer>(redundantIndexes);
		// we'll sort the list so that we can add -1's to the randomIndexesList in order
		// to ensure all of the -1's appear within the batch size.
		Collections.sort(redundantIndexesList);
		System.out.println(redundantIndexesList);
		System.out.println("batch size: " + batchSize);
		for (Integer index : redundantIndexesList) {
			System.out.println("adding -1 at position " + index);
			randomIndexesList.add(index, -1);
		}

		// sanity check -- make sure there are the proper number of -1's in the first n
		// indexes where n is the batch size
		int minusOneCount = 0;
		for (int i = 0; i < randomIndexesList.size(); i++) {
			if (randomIndexesList.get(i) == -1) {
				minusOneCount++;
			}
		}

		if (minusOneCount != redundantSentencesToIncludeCount) {
			throw new IllegalStateException(
					String.format("Observed %d index placeholders for redundant sentences but expected %d.",
							minusOneCount, redundantSentencesToIncludeCount));
		}

		System.out.println("Random index list: " + randomIndexesList.toString());

		return randomIndexesList;
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

	// authentication:
	// https://www.elastic.co/guide/en/elasticsearch/client/java-api-client/current/_other_authentication_methods.html
	/**
	 * @param elasticUrl
	 * @param elasticPort
	 * @param apiKeyAuth
	 * @param indexName
	 * @param maxReturnCount                         - the target number of records
	 *                                               to return
	 * @param ontologyPrefixes
	 * @param ontologyPrefixToAllowableConceptIdsMap
	 * @param biolinkAssociation
	 * @param entityOntologyIdsToExclude
	 * @return
	 * @throws IOException
	 */
	public static Set<TextDocument> search(String elasticUrl, int elasticPort, String apiKeyAuth, String indexName,
			int maxReturnCount, Set<Set<String>> ontologyPrefixes,
			Map<String, Set<String>> ontologyPrefixToAllowableConceptIdsMap, BiolinkAssociation biolinkAssociation,
			Set<String> entityOntologyIdsToExclude, Set<String> alreadyAssignedDocumentIds) throws IOException {

		Set<BiolinkClass> biolinkClasses = new HashSet<BiolinkClass>(
				Arrays.asList(biolinkAssociation.getSubjectClass(), biolinkAssociation.getObjectClass()));

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

			String queryJson = buildSentenceQuery(ontologyPrefixes);
			System.out.println("QUERY:\n" + queryJson);

			Query query = new Query.Builder().withJson(new ByteArrayInputStream(queryJson.getBytes())).build();

			Time.Builder tb = new Time.Builder();
			Time t = tb.time("1m").build();

			SearchRequest.Builder searchBuilder = new SearchRequest.Builder().scroll(t).size(SEARCH_BATCH_SIZE)
					.index(indexName).query(query);
			SearchRequest request = searchBuilder.build();

			SearchResponse<Sentence> response = client.search(request, Sentence.class);

			String scrollId = response.scrollId();
			Set<String> ontologyPrefixesToIncludeInSearchHits = CollectionsUtil.consolidateSets(ontologyPrefixes);
			List<Hit<Sentence>> hits = response.hits().hits();
			System.out.println("Initial hits returned: " + hits.size());
			processHits(ontologyPrefixToAllowableConceptIdsMap, entityOntologyIdsToExclude, biolinkClasses,
					docsToReturn, ontologyPrefixesToIncludeInSearchHits, alreadyAssignedDocumentIds, hits);

			/*
			 * If we get to this point, then we've extracted some documents from the
			 * Elasticsearch instance (max= 10,000). We will check to see if we have
			 * fulfilled the amount requested (maxReturnCount). If we have not, then we loop
			 * using the "scrolling" functionality of Elasticsearch to get more documents
			 * (if there are more).
			 */
			while (docsToReturn.size() < maxReturnCount) {
				System.out.println("DOCS TO RETURN SIZE: " + docsToReturn.size());
				ScrollRequest.Builder scrollRequestBuilder = new ScrollRequest.Builder().scroll(t).scrollId(scrollId);
				ScrollRequest scrollRequest = scrollRequestBuilder.build();
				ScrollResponse<Sentence> scrollResponse = client.scroll(scrollRequest, Sentence.class);
				hits = scrollResponse.hits().hits();
				System.out.println("Supplemental hits returned: " + hits.size());
				if (hits.size() == 0) {
					/* then we have exhausted all hits, so break out of the loop */
					break;
				}
				scrollId = scrollResponse.scrollId();
				processHits(ontologyPrefixToAllowableConceptIdsMap, entityOntologyIdsToExclude, biolinkClasses,
						docsToReturn, ontologyPrefixesToIncludeInSearchHits, alreadyAssignedDocumentIds, hits);
			}

		}
		return docsToReturn;

	}

	/**
	 * @param ontologyPrefixToAllowableConceptIdsMap
	 * @param entityOntologyIdsToExclude
	 * @param biolinkClasses
	 * @param docsToReturn
	 * @param ontologyPrefixesToIncludeInSearchHits
	 * @param alreadyAssignedDocumentIds             a set of document IDs that have
	 *                                               already been assigned for
	 *                                               annotation. We will exclude
	 *                                               returning these since they
	 *                                               would be redundant.
	 * @param hits
	 */
	private static void processHits(Map<String, Set<String>> ontologyPrefixToAllowableConceptIdsMap,
			Set<String> entityOntologyIdsToExclude, Set<BiolinkClass> biolinkClasses, Set<TextDocument> docsToReturn,
			Set<String> ontologyPrefixesToIncludeInSearchHits, Set<String> alreadyAssignedDocumentIds,
			List<Hit<Sentence>> hits) {
		for (Hit<Sentence> hit : hits) {
			TextDocument td = deserializeAnnotatedText(hit.source().getAnnotatedText(),
					ontologyPrefixesToIncludeInSearchHits, ontologyPrefixToAllowableConceptIdsMap);

			td = excludeBasedOnEntityIds(td, entityOntologyIdsToExclude, biolinkClasses);
			if (td != null) {
				/*
				 * only add this sentence to the documents to return if it has not been
				 * previously assigned for annotation
				 */
				if (!alreadyAssignedDocumentIds.contains(td.getSourceid())) {
					/*
					 * there seems to be the occasional document that gets returned without
					 * annotations -- require the documents to have annotations
					 */

					if (td.getAnnotations() != null) {
						docsToReturn.add(td);
					} else {
						System.err.println("Search returned a document with a null annotations field.");
					}
				}
			}
		}
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
		String booleanQueryTemplate = getBooleanQueryTemplateFromClasspath();

		// load annotatedText match template
		String annotatedTextMatchTemplate = getAnnotatedTextMatchTemplateFromClasspath();

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

	@VisibleForTesting
	protected static String getAnnotatedTextMatchTemplateFromClasspath() throws IOException {
		return ClassPathUtil.getContentsFromClasspathResource(ElasticsearchToBratExporter.class,
				ELASTIC_ANNOTATEDTEXT_MATCH_TEMPLATE, UTF8);
	}

	@VisibleForTesting
	protected static String getBooleanQueryTemplateFromClasspath() throws IOException {
		return ClassPathUtil.getContentsFromClasspathResource(ElasticsearchToBratExporter.class,
				ELASTIC_BOOLEAN_QUERY_TEMPLATE, UTF8);
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
	 * @throws IOException
	 */
	@VisibleForTesting
	protected static String createAnnotatedTextMatchStanza(String annotatedTextMatchTemplate,
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

	/**
	 * @param documentId
	 * @param annotatedText
	 * @param ontologyPrefixes                    only keep annotations with the
	 *                                            specified ontology prefixes
	 * @param ontologyPrefixToAllowableConceptIds if not null, then this is a
	 *                                            mapping from ontology prefix to
	 *                                            allowable concept ids. This will
	 *                                            most often be used with the GO_
	 *                                            prefix and sets of
	 *                                            biological_process,
	 *                                            molecular_function, or
	 *                                            cellular_component concept ids.
	 * @return
	 */
	@VisibleForTesting
	public static TextDocument deserializeAnnotatedText(String annotatedText, Set<String> ontologyPrefixes,
			Map<String, Set<String>> ontologyPrefixToAllowableConceptIds) {

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
						String ontologyPrefix = conceptId.substring(0, conceptId.indexOf("_"));
						if (ontologyPrefixes.contains(ontologyPrefix)) {
							String id = conceptId.replace("_", ":");

							// if there are no entries in the map, then no filtering will be done based on
							// the map
							if (ontologyPrefixToAllowableConceptIds == null
									// if there are entries in the map, do the entries apply to the current ontology
									// prefix? If not, then no filtering is done based on the map
									|| (ontologyPrefixToAllowableConceptIds != null
											&& !ontologyPrefixToAllowableConceptIds.containsKey(ontologyPrefix))
									// if there are entries in the map, and the map contains the ontology prefix,
									// and the concept id list contains the concept id then continue.
									|| (ontologyPrefixToAllowableConceptIds != null
											&& ontologyPrefixToAllowableConceptIds.containsKey(ontologyPrefix)
											&& ontologyPrefixToAllowableConceptIds.get(ontologyPrefix).contains(id))) {
								TextAnnotation annotation = factory.createAnnotation(spanStart, spanEnd, coveredText,
										id);
								annots.add(annotation);
							}
//							else {
//								System.out.println("Exclude: " + id + " prefix = " + ontologyPrefix);
//								System.out.println("contains prefix: " + (ontologyPrefixToAllowableConceptIds.containsKey(ontologyPrefix)));
//								System.out.println("contains id: " + (ontologyPrefixToAllowableConceptIds.get(ontologyPrefix).contains(id)));
//							}
						}
					} catch (Exception e) {
						System.err.println("NO UNDERSCORE: " + conceptId);
					}
				}
			}

			annotatedTextOffset = m.end();
			sentenceTextOffset += (m.group().length() - coveredText.length());

		}

		sb.append(decodedAnnotatedText.substring(annotatedTextOffset));
		String sentenceText = sb.toString();
		String sentenceId = computeSentenceIdentifier(sentenceText);

		TextDocument td = new TextDocument(sentenceId, "PubMed", sentenceText);
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
	 * Each new line in the input document is its own sentence. This method splits
	 * all sentences into distinct @link{TextDocument} objects.
	 * 
	 * @param td
	 * @return
	 */
	@VisibleForTesting
	public static List<TextDocument> splitIntoSentences(TextDocument td) {
		Map<Span, TextDocument> spanToSentences = new HashMap<Span, TextDocument>();
		String[] sentences = td.getText().split("\\n");
		int offset = 0;
		for (String sentence : sentences) {
			Span span = new Span(offset, sentence.length() + offset);
			TextDocument sentenceDoc = new TextDocument("sent-id", "sent-source", sentence);
			spanToSentences.put(span, sentenceDoc);

			// verify that the span in the original text matches the sentence
			if (!sentence.equals(td.getText().substring(span.getSpanStart(), span.getSpanEnd()))) {
				throw new IllegalStateException("Sentence text does not match span in original document.");
			}
			offset += (sentence.length() + 1); // +1 for the line break
		}

		// now assign annotations from the original document to the individual
		// sentences. Each annotation should only overlap with a single sentence;
		for (TextAnnotation annot : td.getAnnotations()) {
			for (Entry<Span, TextDocument> entry : spanToSentences.entrySet()) {
				Span sentenceSpan = entry.getKey();
				if (annot.getAggregateSpan().overlaps(sentenceSpan)) {
					// update the span so that it is relative to the sentence not the document
					List<Span> updatedSpans = new ArrayList<Span>();
					for (Span span : annot.getSpans()) {
						updatedSpans.add(new Span(span.getSpanStart() - sentenceSpan.getSpanStart(),
								span.getSpanEnd() - sentenceSpan.getSpanStart()));
					}
					annot.setSpans(updatedSpans);
					entry.getValue().addAnnotation(annot);

					// since the annot will only overlap with a single sentence, we can break the
					// loop once we find an overlapping match
					break;
				}

			}
		}

		// sort output sentences by their span (this is useful for testing purposes)
		List<TextDocument> outputDocs = new ArrayList<TextDocument>();
		Map<Span, TextDocument> sortedMap = CollectionsUtil.sortMapByKeys(spanToSentences, SortOrder.ASCENDING);
		for (Entry<Span, TextDocument> entry : sortedMap.entrySet()) {
			outputDocs.add(entry.getValue());
		}
		return outputDocs;
	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class Sentence {

		@SuppressWarnings("unused")
		public static final String ID = "id";
		@SuppressWarnings("unused")
		public static final String DOCUMENT_ID = "documentId";
		@SuppressWarnings("unused")
		public static final String ANNOTATED_TEXT = "annotatedText";

		private String id;
		private String documentId;
		private String annotatedText;

		public Sentence() {

		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getDocumentId() {
			return documentId;
		}

		public void setDocumentId(String documentId) {
			this.documentId = documentId;
		}

		public String getAnnotatedText() {
			return annotatedText;
		}

		public void setAnnotatedText(String annotatedText) {
			this.annotatedText = annotatedText;
		}

	}

}
