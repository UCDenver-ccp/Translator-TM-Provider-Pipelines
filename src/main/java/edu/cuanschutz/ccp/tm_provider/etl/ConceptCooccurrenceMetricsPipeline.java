package edu.cuanschutz.ccp.tm_provider.etl;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.StreamSupport;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import com.spotify.scio.transforms.RateLimiterDoFn;

import edu.cuanschutz.ccp.tm_provider.etl.fn.ConceptCooccurrenceCountsFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ConceptCooccurrenceCountsFn.ConceptPair;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ConceptCooccurrenceCountsFn.CooccurLevel;
import edu.cuanschutz.ccp.tm_provider.etl.fn.PCollectionUtil;
import edu.cuanschutz.ccp.tm_provider.etl.fn.PCollectionUtil.Delimiter;
import edu.cuanschutz.ccp.tm_provider.etl.util.ConceptCooccurrenceMetrics;
import lombok.Data;

/**
 * Load files containing concept counts from the
 * {@link ConceptCooccurrenceCountsPipelineTest} and computes a number of
 * cooccurrence metrics for each cooccurring concept pair. Results are stored in
 * a database.
 */
public class ConceptCooccurrenceMetricsPipeline {

	private static final Logger LOGGER = Logger.getLogger(ConceptCooccurrenceMetricsPipeline.class.getName());

	protected static final int PUBLICATION_STORAGE_LIMIT = 25;

	@SuppressWarnings("serial")
	public static TupleTag<KV<String, CooccurrencePublication>> PAIR_PUBLICATIONS_TAG = new TupleTag<KV<String, CooccurrencePublication>>() {
	};

	@SuppressWarnings("serial")
	public static TupleTag<KV<String, String>> PAIR_KEY_TO_DOC_ID_TAG = new TupleTag<KV<String, String>>() {
	};

//	@SuppressWarnings("serial")
//	public static TupleTag<CooccurrenceScores> SCORES_TAG = new TupleTag<CooccurrenceScores>() {
//	};

	public interface Options extends DataflowPipelineOptions {

		@Description("Pipe-delimited list of Cooccur levels to process, e.g. DOCUMENT, ABSTRACT, TITLE, SENTENCE")
		@Required
		String getCooccurLevelsToProcess();

		void setCooccurLevelsToProcess(String value);

		@Description("Path to the bucket where the count files are located")
		@Required
		String getCountFileBucket();

		void setCountFileBucket(String bucketPath);

		@Description("if true, concept ancestors are added to the cooccurrence computation when they appear. Note that this increases the computational intensiveness of computing coocccurrence metrics immensely.")
		@Required
		boolean getAddAncestors();

		void setAddAncestors(boolean value);

		@Description("path to (pattern for) the file(s) containing mappings from ontology class to ancestor classes")
		@Required
		String getAncestorMapFilePath();

		void setAncestorMapFilePath(String path);

		@Description("delimiter used to separate columns in the ancestor map file")
		@Required
		Delimiter getAncestorMapFileDelimiter();

		void setAncestorMapFileDelimiter(Delimiter delimiter);

		@Description("delimiter used to separate items in the set in the second column of the ancestor map file")
		@Required
		Delimiter getAncestorMapFileSetDelimiter();

		void setAncestorMapFileSetDelimiter(Delimiter delimiter);

		@Description("The name of the database")
		@Required
		String getDatabaseName();

		void setDatabaseName(String value);

		@Description("The database username")
		@Required
		String getDbUsername();

		void setDbUsername(String value);

		@Description("The password for the corresponding database user")
		@Required
		String getDbPassword();

		void setDbPassword(String value);

		@Description("Cloud SQL MySQL instance name")
		@Required
		String getMySqlInstanceName();

		void setMySqlInstanceName(String value);

		@Description("GCP region for the Cloud SQL instance (see the connection name in the GCP console)")
		@Required
		String getCloudSqlRegion();

		void setCloudSqlRegion(String value);

		@Description("A pipe-delimited list of concept prefixes to include")
		@Required
		String getConceptPrefixesToInclude();

		void setConceptPrefixesToInclude(String value);

	}

	public static void main(String[] args) {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline p = Pipeline.create(options);

		final String dbUsername = options.getDbUsername();
		final String dbPassword = options.getDbPassword();
		final String databaseName = options.getDatabaseName();
		final String cloudSqlInstanceName = options.getMySqlInstanceName();
		final String projectId = options.getProject();
		final String cloudSqlRegion = options.getCloudSqlRegion();

		final String instanceName = String.format("%s:%s:%s", projectId, cloudSqlRegion, cloudSqlInstanceName);
		final String jdbcUrl = String.format(
				"jdbc:mysql://google/%s?cloudSqlInstance=%s&socketFactory=com.google.cloud.sql.mysql.SocketFactory&user=%s&password=%s&useUnicode=true&characterEncoding=UTF-8",
				databaseName, instanceName, dbUsername, dbPassword);

		final DataSourceConfiguration dbConfig = JdbcIO.DataSourceConfiguration.create("com.mysql.cj.jdbc.Driver",
				jdbcUrl);

		// enhance each set of concept IDs by adding all ancestor IDs.
		// load a map from concept IDs to ancestor concept IDs
		final PCollectionView<Map<String, Set<String>>> ancestorMapView = PCollectionUtil.fromKeyToSetTwoColumnFiles(
				"ancestor map", p, options.getAncestorMapFilePath(), options.getAncestorMapFileDelimiter(),
				options.getAncestorMapFileSetDelimiter(), Compression.GZIP).apply(View.<String, Set<String>>asMap());

		Set<CooccurLevel> cooccurLevelsToProcess = new HashSet<CooccurLevel>();
		for (String l : options.getCooccurLevelsToProcess().split("\\|")) {
			cooccurLevelsToProcess.add(CooccurLevel.valueOf(l));
		}

		for (final CooccurLevel level : cooccurLevelsToProcess) {

			// load a file containing a mapping from document identifier to all of the
			// concepts in that particular document. Here, document can refer to an entire
			// document, but it may also refer to a sentence, title, abstract, etc.
			//
			// input format is one document per line
			// DOCUMENT_ID [tab] CONCEPT1_ID|CONCEPT2_ID|...

			String docIdToConceptIdFilePattern = options.getCountFileBucket() + "/"
					+ ConceptCooccurrenceCountsPipeline.getDocumentIdToConceptIdsFileNamePrefix(level) + "*";

			PCollection<KV<String, Set<String>>> textIdToConceptIdCollection = PCollectionUtil
					.fromKeyToSetTwoColumnFiles("load docid to conceptid - " + level.name().toLowerCase(), p,
							docIdToConceptIdFilePattern, ConceptCooccurrenceCountsFn.OUTPUT_FILE_COLUMN_DELIMITER,
							ConceptCooccurrenceCountsFn.OUTPUT_FILE_SET_DELIMITER, Compression.UNCOMPRESSED);

			// then supplement each concept id set with all ancestor ids
			PCollection<KV<String, Set<ConceptId>>> textIdToConceptIdWithAncestorsCollection = addAncestorConceptIds(p,
					level, textIdToConceptIdCollection, options.getAddAncestors(), ancestorMapView);

			// create a mapping from each concept ID to the number of documents in which it
			// was observed, then convert it into a View
			final PCollection<KV<String, Long>> conceptIdToCounts = countConceptObservations(p, level,
					textIdToConceptIdWithAncestorsCollection);
			final PCollectionView<Map<String, Long>> singletonCountMap = conceptIdToCounts
					.apply(View.<String, Long>asMap());

			// calculate the total number of concepts present in all documents
			final PCollectionView<Long> totalConceptCount = countTotalConcepts(p, level, conceptIdToCounts);

			// calculate the total number of documents that were processed
			final PCollectionView<Long> totalDocumentCount = countTotalDocumentsView(p, level,
					textIdToConceptIdWithAncestorsCollection);

			final Set<String> conceptPrefixesToInclude = new HashSet<String>(
					Arrays.asList(options.getConceptPrefixesToInclude().split("\\|")));

			// create a mapping from concept pairs (an identifier representing the pair) to
			// the document IDs in which the pair was observed
			PCollectionTuple pairsAndPubs = computeConceptPairs(p, level, textIdToConceptIdWithAncestorsCollection,
					ancestorMapView, conceptPrefixesToInclude);

			PCollection<KV<String, CooccurrencePublication>> pairKeyToPublications = pairsAndPubs
					.get(PAIR_PUBLICATIONS_TAG);
			PCollection<KV<String, String>> conceptPairIdToTextId = pairsAndPubs.get(PAIR_KEY_TO_DOC_ID_TAG);

			PCollection<CooccurrencePublication> publications = limitPublicationsByPairId(level, pairKeyToPublications);

			PCollection<KV<String, Set<String>>> pairToDocIds = groupByPairId(level, conceptPairIdToTextId);

			/* compute the scores for all concept cooccurrence metrics */
			PCollection<CooccurrenceScores> scores = getConceptIdPairToCooccurrenceMetrics(level, singletonCountMap,
					totalConceptCount, totalDocumentCount, pairToDocIds);

			// IDF should be computed with ancestors. Because using ancestors makes
			// computing the cooccurrence pairs challenging due to scaling issues, IDF
			// computation has been migrated to its own pipeline, see {@link
			// ConceptIdfPipeline}
//			/* compute the inverse document frequency for all concepts */
//			PCollection<KV<String, Double>> conceptIdToIdf = getConceptIdf(totalDocumentCount, conceptIdToCounts,
//					level);

//			///////////////////////
//
//			PCollection<String> idf = conceptIdToIdf.apply(ParDo.of(new DoFn<KV<String, Double>, String>() {
//				private static final long serialVersionUID = 1L;
//
//				@ProcessElement
//				public void processElement(ProcessContext context) {
//					KV<String, Double> element = context.element();
//					context.output(element.getKey() + "\t" + element.getValue());
//				}
//			}));
//
//			idf.apply("idf output - " + level.name().toLowerCase(), TextIO.write()
//					.to("gs://translator-text-workflow-dev_work/output/test-metrics/idf").withSuffix(".tsv"));
//
//			PCollection<String> scoreStr = scores.apply(ParDo.of(new DoFn<CooccurrenceScores, String>() {
//				private static final long serialVersionUID = 1L;
//
//				@ProcessElement
//				public void processElement(ProcessContext context) {
//					CooccurrenceScores scores = context.element();
//
//					String outStr = CollectionsUtil.createDelimitedString(
//							Arrays.asList(scores.getPair().toReproducibleKey(), scores.getNgd(), scores.getPmi(),
//									scores.getNpmi(), scores.getNpmim(), scores.getMd(), scores.getLfmd(),
//									scores.getConceptCount1(), scores.getConceptCount2(), scores.getPairCount()),
//							"\t");
//					context.output(outStr);
//				}
//			}));
//			
//			scoreStr.apply("scores output - " + level.name().toLowerCase(), TextIO.write()
//					.to("gs://translator-text-workflow-dev_work/output/test-metrics/scores").withSuffix(".tsv"));
//
//			///////////////////////

			/* ---- INSERT INTO DATABASE BELOW ---- */

//			final double recordsPerSecond = 3.0/200.0; 
			final double recordsPerSecond = 14.5;

//			/* Insert into concept_idf table */
//			final PCollection<Void> afterConceptIdf = conceptIdToIdf
//					.apply(ParDo.of(new RateLimiterDoFn<>(recordsPerSecond)))
//					.apply("insert concept_idf - " + level.name().toLowerCase(),
//							JdbcIO.<KV<String, Double>>write().withDataSourceConfiguration(dbConfig)
//									.withStatement("INSERT INTO concept_idf (concept_curie,level,idf) \n"
//									// @formatter:off
//							+ "values(?,?,?) ON DUPLICATE KEY UPDATE\n" 
//							+ "    concept_curie = VALUES(concept_curie),\n"
//							+ "    level = VALUES(level),\n"
//							+ "    idf = VALUES(idf)")
//							// @formatter:on
//									.withPreparedStatementSetter(
//											new JdbcIO.PreparedStatementSetter<KV<String, Double>>() {
//												private static final long serialVersionUID = 1L;
//
//												public void setParameters(KV<String, Double> conceptIdToIdf,
//														PreparedStatement query) throws SQLException {
//													query.setString(1, conceptIdToIdf.getKey());
//													query.setString(2, level.name().toLowerCase());
//													query.setDouble(3, conceptIdToIdf.getValue());
//												}
//											})
//									.withResults());

			/* Insert into cooccurrence table */
			PCollection<Void> afterCooccurrenceLoad = scores
//					.apply("waiton concept_idf - " + level.name().toLowerCase(), Wait.on(afterConceptIdf))
					.apply(ParDo.of(new RateLimiterDoFn<>(recordsPerSecond)))
					.apply("insert cooccurrence - " + level.name().toLowerCase(), JdbcIO.<CooccurrenceScores>write()
							.withDataSourceConfiguration(dbConfig)
							.withStatement("INSERT INTO cooccurrence (cooccurrence_id,entity1_curie,entity2_curie) \n"
							// @formatter:off
							+ "values(?,?,?) ON DUPLICATE KEY UPDATE\n" 
							+ "    cooccurrence_id = VALUES(cooccurrence_id),\n"
							+ "    entity1_curie = VALUES(entity1_curie),\n" 
							+ "    entity2_curie = VALUES(entity2_curie)")
							// @formatter:on
							.withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<CooccurrenceScores>() {
								private static final long serialVersionUID = 1L;

								public void setParameters(CooccurrenceScores scores, PreparedStatement query)
										throws SQLException {
									query.setString(1, scores.getCooccurrenceId());
									query.setString(2, scores.getPair().getConceptId1());
									query.setString(3, scores.getPair().getConceptId2());
								}
							}).withResults());

			/* Insert into cooccurrence_scores table */
			PCollection<Void> afterCooccurrenceScoresLoad = scores
					.apply("waiton insert cooccurrence - " + level.name().toLowerCase(), Wait.on(afterCooccurrenceLoad))
					.apply(ParDo.of(new RateLimiterDoFn<>(recordsPerSecond)))
					.apply("insert cooccurrence_scores - " + level.name().toLowerCase(), JdbcIO
							.<CooccurrenceScores>write().withDataSourceConfiguration(dbConfig).withBatchSize(5000)
							.withStatement(
									"INSERT INTO cooccurrence_scores (cooccurrence_id,level,concept1_count,concept2_count,pair_count,ngd,pmi,pmi_norm,pmi_norm_max,mutual_dependence,lfmd) \n"
									// @formatter:off
							+ "values(?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE\n" 
							+ "    cooccurrence_id = VALUES(cooccurrence_id),\n"
							+ "    level = VALUES(level),\n"
							+ "    concept1_count = VALUES(concept1_count),\n" 
							+ "    concept2_count = VALUES(concept2_count),\n"
							+ "    pair_count = VALUES(pair_count),\n"
							+ "    ngd = VALUES(ngd),\n"
							+ "    pmi = VALUES(pmi),\n"
							+ "    pmi_norm = VALUES(pmi_norm),\n"
							+ "    pmi_norm_max = VALUES(pmi_norm_max),\n"
							+ "    mutual_dependence = VALUES(mutual_dependence),\n"
							+ "    lfmd = VALUES(lfmd)")
							// @formatter:on
							.withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<CooccurrenceScores>() {
								private static final long serialVersionUID = 1L;

								public void setParameters(CooccurrenceScores scores, PreparedStatement query)
										throws SQLException {
									query.setString(1, scores.getCooccurrenceId());
									query.setString(2, level.name().toLowerCase());
									query.setLong(3, scores.getConceptCount1());
									query.setLong(4, scores.getConceptCount2());
									query.setLong(5, scores.getPairCount());
									query.setDouble(6, scores.getNgd());
									query.setDouble(7, scores.getPmi());
									query.setDouble(8, scores.getNpmi());
									query.setDouble(9, scores.getNpmim());
									query.setDouble(10, scores.getMd());
									query.setDouble(11, scores.getLfmd());
								}
							}).withResults());

			/* Insert into cooccurrence_publication table */
			publications
					.apply("waiton insert cooccurrence scores - " + level.name().toLowerCase(),
							Wait.on(afterCooccurrenceScoresLoad))
					.apply(ParDo.of(new RateLimiterDoFn<>(recordsPerSecond)))
					.apply("insert cooccurrence_publication - " + level.name().toLowerCase(), JdbcIO
							.<CooccurrencePublication>write().withDataSourceConfiguration(dbConfig).withBatchSize(5000)
							.withStatement("INSERT INTO cooccurrence_publication (cooccurrence_id,level,document_id) \n"
							// @formatter:off
							+ "values(?,?,?) ON DUPLICATE KEY UPDATE\n" 
							+ "    cooccurrence_id = VALUES(cooccurrence_id),\n"
							+ "    level = VALUES(level),\n" 
							+ "    document_id = VALUES(document_id)")
							// @formatter:on
							.withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<CooccurrencePublication>() {
								private static final long serialVersionUID = 1L;

								public void setParameters(CooccurrencePublication pub, PreparedStatement query)
										throws SQLException {
									query.setString(1, pub.getCooccurrenceId());
									query.setString(2, level.name().toLowerCase());
									query.setString(3, pub.getDocumentId());
								}
							}));
		}

		p.run().waitUntilFinish();
	}

	/**
	 * limits the number of publication IDs that get stored for each cooccurrence
	 * pair to PUBLICATION_STORAGE_LIMIT
	 * 
	 * @param level
	 * @param pairIdToPublication
	 * @return
	 */
	protected static PCollection<CooccurrencePublication> limitPublicationsByPairId(final CooccurLevel level,
			PCollection<KV<String, CooccurrencePublication>> pairIdToPublication) {
		PCollection<KV<String, Iterable<CooccurrencePublication>>> col = pairIdToPublication.apply(
				"group-pubs-by-pair - " + level.name().toLowerCase(),
				GroupByKey.<String, CooccurrencePublication>create());

		return col.apply("limit-stored-pubs - " + level.name().toLowerCase(),
				ParDo.of(new DoFn<KV<String, Iterable<CooccurrencePublication>>, CooccurrencePublication>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext context) {
						KV<String, Iterable<CooccurrencePublication>> pairIdToPublications = context.element();
						Iterable<CooccurrencePublication> pubs = pairIdToPublications.getValue();

						Set<CooccurrencePublication> pubSet = new HashSet<CooccurrencePublication>();
						for (CooccurrencePublication pub : pubs) {
							pubSet.add(pub);
							if (pubSet.size() > PUBLICATION_STORAGE_LIMIT) {
								break;
							}
						}

						for (CooccurrencePublication pub : pubSet) {
							context.output(pub);
						}
					}

				}));
	}

	protected static PCollection<KV<String, Set<String>>> groupByPairId(final CooccurLevel level,
			PCollection<KV<String, String>> conceptPairIdToTextId) {
		PCollection<KV<String, Iterable<String>>> col = conceptPairIdToTextId
				.apply("group-by-pair - " + level.name().toLowerCase(), GroupByKey.<String, String>create());

		PCollection<KV<String, Set<String>>> pairToDocIds = col.apply(
				"combine-docid-by-pair - " + level.name().toLowerCase(),
				ParDo.of(new DoFn<KV<String, Iterable<String>>, KV<String, Set<String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext context) {
						KV<String, Iterable<String>> pairIdToDocIds = context.element();
						String pairId = pairIdToDocIds.getKey();
						Iterable<String> docIds = pairIdToDocIds.getValue();

						Set<String> docIdSet = new HashSet<String>();
						for (String docId : docIds) {
							docIdSet.add(docId);
						}

						context.output(KV.of(pairId, docIdSet));
					}

				}));
		return pairToDocIds;
	}

	protected static PCollectionTuple computeConceptPairs(Pipeline p, CooccurLevel level,
			PCollection<KV<String, Set<ConceptId>>> textIdToConceptIdWithAncestorsCollection,
			PCollectionView<Map<String, Set<String>>> ancestorMapView, Set<String> conceptPrefixesToInclude) {

//		PCollection<KV<String, String>> conceptPairIdToTextId = 

		return textIdToConceptIdWithAncestorsCollection.apply("pair concepts - " + level.name().toLowerCase(),
				ParDo.of(new DoFn<KV<String, Set<ConceptId>>, KV<String, String>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext context) {
						KV<String, Set<ConceptId>> documentIdToConceptIds = context.element();
						String documentId = documentIdToConceptIds.getKey();
						Set<ConceptId> conceptIds = documentIdToConceptIds.getValue();

						Set<ConceptPair> pairs = new HashSet<ConceptPair>();
						if (conceptIds.size() > 1) {

							Map<String, Set<String>> ancestorMap = context.sideInput(ancestorMapView);

							// exclude if the concept identifiers are the same or if one of the concepts is
							// an ancestor of the other; if there is an ancestor relationship between a pair
							// of concepts then that pair will always occur together so there's no need to
							// keep track. We will also exclude certain concepts based on prefix for now
							// simply to limit the number of cooccurrences.
							for (ConceptId conceptId1 : conceptIds) {
								for (ConceptId conceptId2 : conceptIds) {
									if (!(excludeConceptIdByPrefix(conceptId1, conceptPrefixesToInclude)
											|| excludeConceptIdByPrefix(conceptId2, conceptPrefixesToInclude))) {
										if (!conceptId1.equals(conceptId2)
												&& !areAncestors(conceptId1, conceptId2, ancestorMap)) {
											ConceptPair pair = new ConceptPair(conceptId1.getId(), conceptId2.getId());

											// only link the pair of concepts that originally appeared in the document
											// with
											// the document ID
											if (conceptId1.isOriginalConcept() && conceptId2.isOriginalConcept()
													&& !pairs.contains(pair)) {

												String docIdToStore = getDocumentIdToStore(documentId);
												// ensure that the document ID will fit in the database column
												if (docIdToStore.length() < 16) {
													CooccurrencePublication pub = new CooccurrencePublication(pair,
															docIdToStore);
													context.output(PAIR_PUBLICATIONS_TAG,
															KV.of(pair.toReproducibleKey(), pub));
												} else {
													LOGGER.log(Level.WARNING,
															"Encountered document ID too long to store for pair: "
																	+ pair.toString() + " -- " + docIdToStore);
//													throw new IllegalStateException(
//															"Encountered document ID too long to store for pair: "
//																	+ pair.toString() + " -- " + docIdToStore);
												}
											}

											pairs.add(pair);
										}
									}
								}
							}
						}

						for (ConceptPair pair : pairs) {
							context.output(PAIR_KEY_TO_DOC_ID_TAG, KV.of(pair.toReproducibleKey(), documentId));
						}
					}

					private boolean excludeConceptIdByPrefix(ConceptId conceptId,
							Set<String> conceptPrefixesToInclude) {

						// if the prefix list is empty, then include all prefixes
						if (conceptPrefixesToInclude == null || conceptPrefixesToInclude.isEmpty()) {
							return false;
						}

						if (conceptPrefixesToInclude != null) {
							for (String prefix : conceptPrefixesToInclude) {
								if (conceptId.getId().startsWith(prefix)) {
									return false;
								}
							}
						}
						return true;
					}

					/**
					 * @param conceptId1
					 * @param conceptId2
					 * @param ancestorMap
					 * @return true if one of the input concept IDs is the ancestor of the other
					 */
					private boolean areAncestors(ConceptId conceptId1, ConceptId conceptId2,
							Map<String, Set<String>> ancestorMap) {

						Set<String> concept1Ancestors = ancestorMap.get(conceptId1.getId());
						if (concept1Ancestors != null && concept1Ancestors.contains(conceptId2.getId())) {
							return true;
						}

						Set<String> concept2Ancestors = ancestorMap.get(conceptId2.getId());
						if (concept2Ancestors != null && concept2Ancestors.contains(conceptId1.getId())) {
							return true;
						}

						return false;

					}

				}).withOutputTags(PAIR_KEY_TO_DOC_ID_TAG, TupleTagList.of(PAIR_PUBLICATIONS_TAG))
						.withSideInputs(ancestorMapView));

	}

	/**
	 * The document ID may be something like PMID:12345678, but it may also be
	 * something like
	 * PMID:208421_title_3593a05b85c4d250540e29ee7c16addf32ab026eb2461005f40234d4fa4623c9.
	 * We only want to store the base document ID as the title and sentence IDs, for
	 * example, are only used internally in this code base and have no external
	 * meaning.
	 * 
	 * @param documentId
	 * @param level
	 * @return
	 */
	protected static String getDocumentIdToStore(String documentId) {
		// just return the first part of the document ID which should be the
		// "base" id, e.g. PMID.
		return documentId.split("_")[0];
	}

	/**
	 * counts all unique document IDs (keys) in the input PCollection
	 * 
	 * @param p
	 * @param level
	 * @param textIdToConceptIdWithAncestorsCollection
	 * @return
	 */
	protected static PCollection<Long> countTotalDocuments(Pipeline p, CooccurLevel level,
			PCollection<KV<String, Set<ConceptId>>> textIdToConceptIdWithAncestorsCollection) {
		// dedup in case some documents got processed multiple times
		PCollection<KV<String, Iterable<Set<ConceptId>>>> nonredundant = textIdToConceptIdWithAncestorsCollection
				.apply("group-by-key", GroupByKey.<String, Set<ConceptId>>create());
		PCollection<String> documentIds = nonredundant.apply(Keys.<String>create());
		return documentIds.apply("count total docs - " + level.name().toLowerCase(), Count.globally());

	}

	public static PCollectionView<Long> countTotalDocumentsView(Pipeline p, CooccurLevel level,
			PCollection<KV<String, Set<ConceptId>>> textIdToConceptIdWithAncestorsCollection) {
		return countTotalDocuments(p, level, textIdToConceptIdWithAncestorsCollection).apply(View.asSingleton());
	}

	/**
	 * sums up all concept counts in the input map
	 * 
	 * @param p
	 * @param level
	 * @param conceptIdToCounts
	 * @return
	 */
	protected static PCollectionView<Long> countTotalConcepts(Pipeline p, CooccurLevel level,
			PCollection<KV<String, Long>> conceptIdToCounts) {
		// dedup in case some concepts got processed multiple times
		PCollection<Long> conceptCounts = PipelineMain.deduplicateByKey(conceptIdToCounts);
		return conceptCounts.apply("count total concepts - " + level.name().toLowerCase(),
				Sum.longsGlobally().asSingletonView());
	}

	/**
	 * take the input mapping from document ID to concept ID and return a mapping
	 * from concept ID to the count of the number of documents in which that concept
	 * appears
	 * 
	 * @param p
	 * @param level
	 * @param textIdToConceptIdWithAncestorsCollection
	 * @return
	 */
	protected static PCollection<KV<String, Long>> countConceptObservations(Pipeline p, CooccurLevel level,
			PCollection<KV<String, Set<ConceptId>>> textIdToConceptIdWithAncestorsCollection) {

		PCollection<KV<String, String>> conceptIdToDocumentIdMapping = textIdToConceptIdWithAncestorsCollection.apply(
				"count concepts - " + level.name().toLowerCase(),
				ParDo.of(new DoFn<KV<String, Set<ConceptId>>, KV<String, String>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext context) {
						KV<String, Set<ConceptId>> documentIdToConceptIds = context.element();
						String documentId = documentIdToConceptIds.getKey();
						Set<ConceptId> conceptIds = documentIdToConceptIds.getValue();
						for (ConceptId conceptId : conceptIds) {
							context.output(KV.of(conceptId.getId(), documentId));
						}
					}
				}));

		// group by concept-id so that we now map from concept-id to all of its
		// content-ids
		PCollection<KV<String, Iterable<String>>> conceptIdToDocIds = conceptIdToDocumentIdMapping
				.apply("group-by-concept-id", GroupByKey.<String, String>create());
		// return mapping of concept id to the number of documents (or sentences, etc.)
		// in which it was observed
		PCollection<KV<String, Long>> conceptIdToCounts = conceptIdToDocIds
				.apply(ParDo.of(new DoFn<KV<String, Iterable<String>>, KV<String, Long>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						KV<String, Iterable<String>> element = c.element();

						long count = StreamSupport.stream(element.getValue().spliterator(), false).count();

						c.output(KV.of(element.getKey(), count));
					}
				}));

		return conceptIdToCounts;

	}

	/**
	 * augment the input concept IDs with all ancestor IDs
	 * 
	 * @param p
	 * @param level
	 * @param textIdToConceptIdCollection
	 * @param ancestorMapView
	 * @return
	 */
	protected static PCollection<KV<String, Set<ConceptId>>> addAncestorConceptIds(Pipeline p, CooccurLevel level,
			PCollection<KV<String, Set<String>>> textIdToConceptIdCollection, boolean addAncestors,
			PCollectionView<Map<String, Set<String>>> ancestorMapView) {

		return textIdToConceptIdCollection.apply("add ancestors - " + level.name().toLowerCase(),
				ParDo.of(new DoFn<KV<String, Set<String>>, KV<String, Set<ConceptId>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext context) {
						Map<String, Set<String>> ancestorMap = context.sideInput(ancestorMapView);

						KV<String, Set<String>> documentIdToConceptIds = context.element();
						String documentId = documentIdToConceptIds.getKey();
						Set<ConceptId> conceptIds = new HashSet<ConceptId>();
						for (String id : documentIdToConceptIds.getValue()) {
							conceptIds.add(new ConceptId(id, true));
						}

						if (addAncestors) {
							Set<ConceptId> ancestorIds = new HashSet<ConceptId>();
							for (String conceptId : documentIdToConceptIds.getValue()) {
								String conceptPrefix = null;
								if (conceptId.contains(":")) {
									conceptPrefix = conceptId.substring(0, conceptId.indexOf(":"));
								}
								if (ancestorMap.containsKey(conceptId)) {
									Set<String> ancestors = ancestorMap.get(conceptId);
									for (String ancestorId : ancestors) {
										// in case there are any blank id's exclude them -- this was observed at one
										// time. Also, avoid adding ancestors that have a different prefix. This avoids,
										// for example, adding upper-level BFO concepts that are ancestors of GO
										// concepts as we have no real need for upper-level concepts in our application.
										// This also limits the amount of concepts that will be used to generate concept
										// pairs which will reduce, albeit only slightly, the computational load of
										// computing pairs.
										if (!ancestorId.trim().isEmpty()
												&& (conceptPrefix == null || ancestorId.startsWith(conceptPrefix))) {
											ancestorIds.add(new ConceptId(ancestorId, false));
										}
									}
								}
							}

							conceptIds.addAll(ancestorIds);
						}

						context.output(KV.of(documentId, conceptIds));
					}
				}).withSideInputs(ancestorMapView));

	}

//	/**
//	 * Creates a file pattern given the prefix and {@link CooccurLevel}
//	 * 
//	 * @param filePrefix
//	 * @param level
//	 * @return
//	 */
//	private static String getFilePattern(String filePrefix, CooccurLevel level) {
//		String period = "";
//		if (!filePrefix.endsWith(".")) {
//			period = ".";
//		}
//		return filePrefix + period + level.name().toLowerCase() + ".*";
//	}

	private static PCollection<CooccurrenceScores> getConceptIdPairToCooccurrenceMetrics(CooccurLevel level,
			final PCollectionView<Map<String, Long>> singletonCountMap,
			final PCollectionView<Long> totalConceptCountView, PCollectionView<Long> totalDocumentCountView,
			PCollection<KV<String, Set<String>>> pairToDocIds) {
		return pairToDocIds.apply("metrics - " + level.name().toLowerCase(),
				ParDo.of(new DoFn<KV<String, Set<String>>, CooccurrenceScores>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						KV<String, Set<String>> element = c.element();
						ConceptPair pair = ConceptPair.fromReproducibleKey(element.getKey());
						long totalConceptCount = c.sideInput(totalConceptCountView);
						long totalDocumentCount = c.sideInput(totalDocumentCountView);

						Map<String, Long> conceptCountMap = c.sideInput(singletonCountMap);

						Long xConceptCount = conceptCountMap.get(pair.getConceptId1());
						Long yConceptCount = conceptCountMap.get(pair.getConceptId2());

						// commented out as the pairCount is now incorporated in the output of the
						// publications
						// Long pairCount = StreamSupport.stream(element.getValue().spliterator(),
						// false).count();

						Iterable<String> documentIdIterable = element.getValue();
						long pairCount = StreamSupport.stream(documentIdIterable.spliterator(), false).count();
//						for (String documentId : documentIdIterable) {
//							// if the level is DOCUMENT then the documentId should simply be the id of the
//							// document, e.g. PMID:12345678, however if the level is something other than
//							// DOCUMENT, then the documentId will be a mashup of the documentId_level_hash
//							// where the hash is a unique id for part of a document, e.g. sentence, title.
//							// Since the hash ID is internal to our database, we only need to output the
//							// document id itself, so we parse the documentId to extract it.
//
//							// Also, we are only going to save a sample of the publications. There are way
//							// too many otherwise. So we'll save up to 2 for each pair.
//							if (pairCount < 2) {
//								String docId = documentId;
//								if (level != CooccurLevel.DOCUMENT) {
//									if (documentId.contains("_")) {
//										docId = documentId.substring(0, documentId.indexOf("_"));
//									}
//								}
//								CooccurrencePublication pub = new CooccurrencePublication(pair, docId);
//								c.output(PAIR_PUBLICATIONS_TAG, pub);
//							}
//							pairCount++;
//						}

						if (xConceptCount == null) {
							LOGGER.log(Level.WARNING,
									String.format("Unable to find concept count for id: %s", pair.getConceptId1()));
						}
						if (yConceptCount == null) {
							LOGGER.log(Level.WARNING,
									String.format("Unable to find concept count for id: %s", pair.getConceptId2()));
						}

						if (xConceptCount != null && yConceptCount != null) {
							double ngd = ConceptCooccurrenceMetrics.normalizedGoogleDistance(xConceptCount,
									yConceptCount, pairCount, totalConceptCount);
							double pmi = ConceptCooccurrenceMetrics.pointwiseMutualInformation(totalDocumentCount,
									xConceptCount, yConceptCount, pairCount);
							double npmi = ConceptCooccurrenceMetrics.normalizedPointwiseMutualInformation(
									totalDocumentCount, xConceptCount, yConceptCount, pairCount);
							double npmim = ConceptCooccurrenceMetrics.normalizedPointwiseMutualInformationMaxDenom(
									totalDocumentCount, xConceptCount, yConceptCount, pairCount);
							double md = ConceptCooccurrenceMetrics.mutualDependence(totalDocumentCount, xConceptCount,
									yConceptCount, pairCount);
							double lfmd = ConceptCooccurrenceMetrics.logFrequencyBiasedMutualDependence(
									totalDocumentCount, xConceptCount, yConceptCount, pairCount);

							// limit number of decimal places so they can be stored in the database

							BigDecimal ngdD = new BigDecimal(ngd).setScale(8, BigDecimal.ROUND_HALF_UP);
							BigDecimal pmiD = new BigDecimal(pmi).setScale(8, BigDecimal.ROUND_HALF_UP);
							BigDecimal npmiD = new BigDecimal(npmi).setScale(8, BigDecimal.ROUND_HALF_UP);
							BigDecimal npmimD = new BigDecimal(npmim).setScale(8, BigDecimal.ROUND_HALF_UP);
							BigDecimal mdD = new BigDecimal(md).setScale(8, BigDecimal.ROUND_HALF_UP);
							BigDecimal lfmdD = new BigDecimal(lfmd).setScale(8, BigDecimal.ROUND_HALF_UP);

							CooccurrenceScores scores = new CooccurrenceScores(pair, xConceptCount, yConceptCount,
									pairCount, ngdD.doubleValue(), pmiD.doubleValue(), npmiD.doubleValue(),
									npmimD.doubleValue(), mdD.doubleValue(), lfmdD.doubleValue());
							c.output(scores);
						}
					}

				}).withSideInputs(singletonCountMap, totalConceptCountView, totalDocumentCountView));

	}

	@Data
	private static class CooccurrenceScores implements Serializable {
		private static final long serialVersionUID = 1L;

		private final ConceptPair pair;
		private final long conceptCount1;
		private final long conceptCount2;
		private final long pairCount;
		private final double ngd;
		private final double pmi;
		private final double npmi;
		private final double npmim;
		private final double md;
		private final double lfmd;

		public String getCooccurrenceId() {
			return pair.getPairId();
		}
	}

	@Data
	protected static class CooccurrencePublication implements Serializable {
		private static final long serialVersionUID = 1L;

		private final ConceptPair pair;
		private final String documentId;

		public String getCooccurrenceId() {
			return pair.getPairId();
		}
	}

	@DefaultCoder(SerializableCoder.class)
	@Data
	protected static class ConceptId implements Serializable {
		private static final long serialVersionUID = 1L;

		private final String id;
		/**
		 * set to true if this concept was explicitly observed in the text. If the
		 * concept represents an ancestor, set it to false;
		 */
		private final boolean isOriginalConcept;
	}

}
