package edu.cuanschutz.ccp.tm_provider.etl;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.StreamSupport;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import edu.cuanschutz.ccp.tm_provider.etl.fn.ConceptCooccurrenceCountsFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ConceptCooccurrenceCountsFn.ConceptPair;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ConceptCooccurrenceCountsFn.CooccurLevel;
import edu.cuanschutz.ccp.tm_provider.etl.fn.PCollectionUtil;
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

	@SuppressWarnings("serial")
	public static TupleTag<CooccurrencePublication> PAIR_PUBLICATIONS_TAG = new TupleTag<CooccurrencePublication>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<CooccurrenceScores> SCORES_TAG = new TupleTag<CooccurrenceScores>() {
	};

	public interface Options extends DataflowPipelineOptions {

		@Description("Pipe-delimited list of Cooccur levels to process, e.g. DOCUMENT, ABSTRACT, TITLE, SENTENCE")
		String getCooccurLevelsToProcess();

		void setCooccurLevelsToProcess(String value);

		@Description("Path to the bucket where the count files are located")
		String getCountFileBucket();

		void setCountFileBucket(String bucketPath);

		@Description("The name of the database")
		String getDatabaseName();

		void setDatabaseName(String value);

		@Description("The database username")
		String getDbUsername();

		void setDbUsername(String value);

		@Description("The password for the corresponding database user")
		String getDbPassword();

		void setDbPassword(String value);

		@Description("Cloud SQL MySQL instance name")
		String getMySqlInstanceName();

		void setMySqlInstanceName(String value);

		@Description("GCP region for the Cloud SQL instance (see the connection name in the GCP console)")
		String getCloudSqlRegion();

		void setCloudSqlRegion(String value);

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

		Set<CooccurLevel> cooccurLevelsToProcess = new HashSet<CooccurLevel>();
		for (String l : options.getCooccurLevelsToProcess().split("\\|")) {
			cooccurLevelsToProcess.add(CooccurLevel.valueOf(l));
		}

		for (final CooccurLevel level : cooccurLevelsToProcess) {
			String conceptIdToCountFilePrefix = options.getCountFileBucket() + "/"
					+ ConceptCooccurrenceCountsPipeline.getConceptIdToLevelFileNamePrefix(level) + "*";
			String levelToConceptCountFilePrefix = options.getCountFileBucket() + "/"
					+ ConceptCooccurrenceCountsPipeline.getLevelToConceptCountFileNamePrefix(level) + "*";
			String conceptPairToLevelFilePrefix = options.getCountFileBucket() + "/"
					+ ConceptCooccurrenceCountsPipeline.getConceptPairToLevelFileNamePrefix(level) + "*";

			final PCollection<KV<String, Long>> conceptIdToCounts = getSingletonCountMapView(conceptIdToCountFilePrefix,
					level, p);

			final PCollectionView<Map<String, Long>> singletonCountMap = conceptIdToCounts
					.apply(View.<String, Long>asMap());

			final PCollectionView<Long> totalConceptCount = getTotalConceptCountView(levelToConceptCountFilePrefix,
					level, p);

			final PCollectionView<Long> totalDocumentCount = getTotalDocumentCountView(levelToConceptCountFilePrefix,
					level, p);

			PCollection<KV<String, String>> pairToDocId = PCollectionUtil.fromTwoColumnFiles(
					"pair file - " + level.name().toLowerCase(), p, conceptPairToLevelFilePrefix,
					ConceptCooccurrenceCountsFn.OUTPUT_FILE_DELIMITER, Compression.UNCOMPRESSED);

			PCollection<KV<String, Iterable<String>>> pairToDocIds = pairToDocId
					.apply("group-by-concept-id-" + level.name().toLowerCase(), GroupByKey.<String, String>create());

			/* compute the scores for all concept cooccurrence metrics */
			PCollectionTuple scoresAndPubs = getConceptIdPairToCooccurrenceMetrics(level, singletonCountMap,
					totalConceptCount, totalDocumentCount, pairToDocIds);

			PCollection<CooccurrenceScores> scores = scoresAndPubs.get(SCORES_TAG);
			PCollection<CooccurrencePublication> publications = scoresAndPubs.get(PAIR_PUBLICATIONS_TAG);

			/* compute the inverse document frequency for all concepts */
			PCollection<KV<String, Double>> conceptIdToIdf = getConceptIdf(totalDocumentCount, conceptIdToCounts,
					level);

			/* ---- INSERT INTO DATABASE BELOW ---- */

			/* Insert into concept_idf table */
			// @formatter:off
			conceptIdToIdf.apply("insert concept_idf - " + level.name().toLowerCase(), JdbcIO.<KV<String, Double>>write().withDataSourceConfiguration(dbConfig)
					.withStatement("INSERT INTO concept_idf (concept_curie,level,idf) \n"
							+ "values(?,?,?) ON DUPLICATE KEY UPDATE\n" 
							+ "    concept_curie = VALUES(concept_curie),\n"
							+ "    level = VALUES(level),\n"
							+ "    idf = VALUES(idf)")
					.withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<KV<String, Double>>() {
						private static final long serialVersionUID = 1L;
	
						public void setParameters(KV<String, Double> conceptIdToIdf, PreparedStatement query) throws SQLException {
							query.setString(1, conceptIdToIdf.getKey());
							query.setString(2, level.name().toLowerCase());
							query.setDouble(3, conceptIdToIdf.getValue());
						}
					}));
			// @formatter:on

			/* Insert into cooccurrence table */
			// @formatter:off
			scores.apply("insert cooccurrence - " + level.name().toLowerCase(), JdbcIO.<CooccurrenceScores>write().withDataSourceConfiguration(dbConfig)
					.withStatement("INSERT INTO cooccurrence (cooccurrence_id,entity1_curie,entity2_curie) \n"
							+ "values(?,?,?) ON DUPLICATE KEY UPDATE\n" 
							+ "    cooccurrence_id = VALUES(cooccurrence_id),\n"
							+ "    entity1_curie = VALUES(entity1_curie),\n" 
							+ "    entity2_curie = VALUES(entity2_curie)")
					.withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<CooccurrenceScores>() {
						private static final long serialVersionUID = 1L;
	
						public void setParameters(CooccurrenceScores scores, PreparedStatement query) throws SQLException {
							query.setString(1, scores.getCooccurrenceId());
							query.setString(2, scores.getPair().getConceptId1());
							query.setString(3, scores.getPair().getConceptId2());
						}
					}));
			// @formatter:on

			/* Insert into cooccurrence_scores table */
			// @formatter:off
			scores.apply("insert cooccurrence_scores - " + level.name().toLowerCase(), JdbcIO.<CooccurrenceScores>write().withDataSourceConfiguration(dbConfig)
					.withStatement("INSERT INTO cooccurrence_scores (cooccurrence_id,level,concept1_count,concept2_count,pair_count,ngd,pmi,pmi_norm,pmi_norm_max,mutual_dependence,lfmd) \n"
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
					.withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<CooccurrenceScores>() {
						private static final long serialVersionUID = 1L;
	
						public void setParameters(CooccurrenceScores scores, PreparedStatement query) throws SQLException {
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
					}));
			// @formatter:on

			/* Insert into cooccurrence_publication table */
			// @formatter:off
			publications.apply("insert cooccurrence_publication - " + level.name().toLowerCase(), JdbcIO.<CooccurrencePublication>write()
					.withDataSourceConfiguration(dbConfig)
					.withStatement("INSERT INTO cooccurrence_publication (cooccurrence_id,level,document_id) \n"
							+ "values(?,?,?) ON DUPLICATE KEY UPDATE\n" 
							+ "    cooccurrence_id = VALUES(cooccurrence_id),\n"
							+ "    level = VALUES(level),\n" 
							+ "    document_id = VALUES(document_id)")
					.withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<CooccurrencePublication>() {
						private static final long serialVersionUID = 1L;
	
						public void setParameters(CooccurrencePublication pub, PreparedStatement query)
								throws SQLException {
							query.setString(1, pub.getCooccurrenceId());
							query.setString(2, level.name().toLowerCase());
							query.setString(3, pub.getDocumentId());
						}
					}));
			// @formatter:on
		}

		p.run().waitUntilFinish();
	}

	/**
	 * @param totalDocumentCountView
	 * @param conceptIdToCounts
	 * @return KV pairs linking the concept ID to the inverse document frequency
	 */
	private static PCollection<KV<String, Double>> getConceptIdf(PCollectionView<Long> totalDocumentCountView,
			PCollection<KV<String, Long>> conceptIdToCounts, CooccurLevel level) {
		return conceptIdToCounts.apply("idf - " + level.name().toLowerCase(),
				ParDo.of(new DoFn<KV<String, Long>, KV<String, Double>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						KV<String, Long> conceptIdToCount = c.element();
						String conceptId = conceptIdToCount.getKey();
						long count = conceptIdToCount.getValue();
						long totalDocumentCount = c.sideInput(totalDocumentCountView);
						double idf = Math.log((double) totalDocumentCount / (double) count);
						c.output(KV.of(conceptId, idf));
					}
				}).withSideInputs(totalDocumentCountView));
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

	private static PCollectionTuple getConceptIdPairToCooccurrenceMetrics(CooccurLevel level,
			final PCollectionView<Map<String, Long>> singletonCountMap,
			final PCollectionView<Long> totalConceptCountView, PCollectionView<Long> totalDocumentCountView,
			PCollection<KV<String, Iterable<String>>> pairToDocIds) {
		return pairToDocIds.apply("metrics - " + level.name().toLowerCase(),
				ParDo.of(new DoFn<KV<String, Iterable<String>>, CooccurrencePublication>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						KV<String, Iterable<String>> element = c.element();
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
						long pairCount = 0;
						for (String documentId : documentIdIterable) {
							// if the level is DOCUMENT then the documentId should simply be the id of the
							// document, e.g. PMID:12345678, however if the level is something other than
							// DOCUMENT, then the documentId will be a mashup of the documentId_level_hash
							// where the hash is a unique id for part of a document, e.g. sentence, title.
							// Since the hash ID is internal to our database, we only need to output the
							// document id itself, so we parse the documentId to extract it.
							String docId = documentId;
							if (level != CooccurLevel.DOCUMENT) {
								if (documentId.contains("_")) {
									docId = documentId.substring(0, documentId.indexOf("_"));
								}
							}
							CooccurrencePublication pub = new CooccurrencePublication(pair, docId);
							c.output(PAIR_PUBLICATIONS_TAG, pub);
							pairCount++;
						}

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

							CooccurrenceScores scores = new CooccurrenceScores(pair, xConceptCount, yConceptCount,
									pairCount, ngd, pmi, npmi, npmim, md, lfmd);
							c.output(SCORES_TAG, scores);
						}
					}

				}).withOutputTags(PAIR_PUBLICATIONS_TAG, TupleTagList.of(SCORES_TAG)).withSideInputs(singletonCountMap,
						totalConceptCountView, totalDocumentCountView));

	}

//	private static PCollection<String> createKgxEdgeLines(
//			final PCollectionView<Map<String, String>> conceptIdToLabelMap,
//			final PCollectionView<Map<String, String>> conceptIdToCategoryMap,
//			PCollection<KV<String, Double>> pairToNgd) {
//		PCollection<String> edgeTsv = pairToNgd.apply(ParDo.of(new DoFn<KV<String, Double>, String>() {
//			private static final long serialVersionUID = 1L;
//
//			@ProcessElement
//			public void processElement(ProcessContext c) {
//				KV<String, Double> pairToNgd = c.element();
//
//				String pairKey = pairToNgd.getKey();
//				ConceptPair pair = ConceptPair.fromReproducibleKey(pairKey);
//
//				String subjectId = pair.getConceptId1();
//				String objectId = pair.getConceptId2();
//				String edgeLabel = KGX_OUTPUT_EDGE_LABEL;
//				String relation = KGX_OUTPUT_RELATION;
//				String associationType = KGX_ASSOCIATION_TYPE;
//				Double ngd = pairToNgd.getValue();
//				String associationId = DigestUtils.sha256Hex(pairKey);
//
//				String kgxEdgeStr = String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s", subjectId, edgeLabel, objectId,
//						relation, associationId, associationType, ngd);
//
//				c.output(kgxEdgeStr);
//			}
//
//		}).withSideInputs(conceptIdToLabelMap, conceptIdToCategoryMap));
//		return edgeTsv;
//	}

//	private static PCollection<String> createKgxNodeLines(final PCollection<KV<String, Long>> conceptIdToCounts,
//			final PCollectionView<Map<String, String>> conceptIdToLabelMap,
//			final PCollectionView<Map<String, String>> conceptIdToCategoryMap) {
//		PCollection<String> nodeTsv = conceptIdToCounts.apply(Keys.<String>create())
//				.apply(ParDo.of(new DoFn<String, String>() {
//					private static final long serialVersionUID = 1L;
//
//					@ProcessElement
//					public void processElement(ProcessContext c) {
//						String conceptId = c.element();
//						Map<String, String> conceptLabelMap = c.sideInput(conceptIdToLabelMap);
//						Map<String, String> conceptCategoryMap = c.sideInput(conceptIdToCategoryMap);
//
//						String label = conceptLabelMap.get(conceptId);
//						String category = conceptCategoryMap.get(conceptId);
//
//						if (label == null) {
//							label = "UKNOWN";
//						}
//						if (category == null) {
//							category = "UKNOWN";
//						}
//						String kgxNodeStr = String.format("%s\t%s\t%s", conceptId, label, category);
//
//						c.output(kgxNodeStr);
//					}
//
//				}).withSideInputs(conceptIdToLabelMap, conceptIdToCategoryMap));
//		return nodeTsv;
//	}

	private static PCollectionView<Long> getTotalConceptCountView(String docIdToConceptCountFilePattern,
			CooccurLevel level, Pipeline p) {
		// compute the total number of concepts observed (N)
		PCollection<KV<String, String>> docIdToConceptCountStr = PCollectionUtil.fromTwoColumnFiles(
				"total concept count - " + level.name().toLowerCase(), p, docIdToConceptCountFilePattern,
				ConceptCooccurrenceCountsFn.OUTPUT_FILE_DELIMITER, Compression.UNCOMPRESSED);
		PCollection<KV<String, Long>> docIdToConceptCount = docIdToConceptCountStr
				.apply(ParDo.of(new DoFn<KV<String, String>, KV<String, Long>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						KV<String, String> element = c.element();
						c.output(KV.of(element.getKey(), Long.parseLong(element.getValue())));
					}
				}));
		// dedup in case some documents got processed multiple times
		PCollection<Long> conceptCounts = PipelineMain.deduplicateByKey(docIdToConceptCount);
		final PCollectionView<Long> totalConceptCount = conceptCounts.apply(Sum.longsGlobally().asSingletonView());
		return totalConceptCount;
	}

	private static PCollectionView<Long> getTotalDocumentCountView(String docIdToConceptCountFilePattern,
			CooccurLevel level, Pipeline p) {
		// compute the total number of concepts observed (N)
		PCollection<KV<String, String>> docIdToConceptCountStr = PCollectionUtil.fromTwoColumnFiles(
				"total doc count - " + level.name().toLowerCase(), p, docIdToConceptCountFilePattern,
				ConceptCooccurrenceCountsFn.OUTPUT_FILE_DELIMITER, Compression.UNCOMPRESSED);

		// dedup in case some documents got processed multiple times
		PCollection<KV<String, Iterable<String>>> nonredundant = docIdToConceptCountStr.apply("group-by-key",
				GroupByKey.<String, String>create());
		PCollection<String> documentIds = nonredundant.apply(Keys.<String>create());

		final PCollectionView<Long> totalDocumentCount = documentIds.apply(Count.globally()).apply(View.asSingleton());
		return totalDocumentCount;
	}

	public static PCollection<KV<String, Long>> getSingletonCountMapView(String singletonFilePattern,
			CooccurLevel level, Pipeline p) {
		// get lines that link concept identifiers to content identifiers (could be a
		// document id, but could also be a sentence id, or something else).
		PCollection<KV<String, String>> conceptIdToDocId = PCollectionUtil.fromTwoColumnFiles(
				"singletons " + level.name().toLowerCase(), p, singletonFilePattern,
				ConceptCooccurrenceCountsFn.OUTPUT_FILE_DELIMITER, Compression.UNCOMPRESSED);
		// group by concept-id so that we now map from concept-id to all of its
		// content-ids
		PCollection<KV<String, Iterable<String>>> conceptIdToDocIds = conceptIdToDocId.apply("group-by-concept-id",
				GroupByKey.<String, String>create());
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
	private static class CooccurrencePublication implements Serializable {
		private static final long serialVersionUID = 1L;

		private final ConceptPair pair;
		private final String documentId;

		public String getCooccurrenceId() {
			return pair.getPairId();
		}
	}

}
