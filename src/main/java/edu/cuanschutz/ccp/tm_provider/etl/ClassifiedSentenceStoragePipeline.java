package edu.cuanschutz.ccp.tm_provider.etl;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RetryConfiguration;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;

import edu.cuanschutz.ccp.tm_provider.etl.fn.ClassifiedSentenceStorageSqlValuesFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ClassifiedSentenceStorageSqlValuesFn.AssertionTableValues;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ClassifiedSentenceStorageSqlValuesFn.AssertionTableValuesCoder;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ClassifiedSentenceStorageSqlValuesFn.EntityTableValues;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ClassifiedSentenceStorageSqlValuesFn.EntityTableValuesCoder;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ClassifiedSentenceStorageSqlValuesFn.EvidenceScoreTableValues;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ClassifiedSentenceStorageSqlValuesFn.EvidenceScoreTableValuesCoder;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ClassifiedSentenceStorageSqlValuesFn.EvidenceTableValues;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ClassifiedSentenceStorageSqlValuesFn.EvidenceTableValuesCoder;
import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.BiolinkAssociation;

/**
 * Combines BERT output (classified sentences) with sentence metadata and stores
 * results in a Cloud SQL instance
 */
public class ClassifiedSentenceStoragePipeline {

	public interface Options extends DataflowPipelineOptions {
		@Description("GCS path to the BERT output file")
		String getBertOutputFilePath();

		void setBertOutputFilePath(String path);

		@Description("GCS path to the sentence metadata file")
		String getSentenceMetadataFilePath();

		void setSentenceMetadataFilePath(String path);

		@Description("The Biolink Association that is being processed.")
		BiolinkAssociation getBiolinkAssociation();

		void setBiolinkAssociation(BiolinkAssociation assoc);

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

		@Description("The minimum BERT classification score required for a sentence to be kept as evidence for an assertion.")
		double getBertScoreInclusionMinimumThreshold();

		void setBertScoreInclusionMinimumThreshold(double minThreshold);

//		@Description("If set, only documents that end with the specified ID suffix will be processed. This allows large loads to be spread out over multiple runs.")
//		String getIdSuffixToProcess();
//
//		void setIdSuffixToProcess(String value);

	}

	public static void main(String[] args) {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline p = Pipeline.create(options);

		
		p.getCoderRegistry().registerCoderForClass(AssertionTableValues.class, new AssertionTableValuesCoder());
		p.getCoderRegistry().registerCoderForClass(EvidenceTableValues.class, new EvidenceTableValuesCoder());
		p.getCoderRegistry().registerCoderForClass(EntityTableValues.class, new EntityTableValuesCoder());
		p.getCoderRegistry().registerCoderForClass(EvidenceScoreTableValues.class, new EvidenceScoreTableValuesCoder());
		
		System.out.println("bert output file: " + options.getBertOutputFilePath());
		System.out.println("sentence metadata file: " + options.getSentenceMetadataFilePath());

		final double bertScoreInclusionMinimumThreshold = options.getBertScoreInclusionMinimumThreshold();

		final String bertOutputFilePath = options.getBertOutputFilePath();
		PCollection<String> bertOutputLines = p.apply(TextIO.read().from(bertOutputFilePath));
		PCollection<KV<String, String>> idToBertOutputLines = getKV(bertOutputLines, 0);

		final String sentenceMetadataFilePath = options.getSentenceMetadataFilePath();
		PCollection<String> metadataLines = p.apply(TextIO.read().from(sentenceMetadataFilePath));
		PCollection<KV<String, String>> idToMetadataLines = getKV(metadataLines, 0);

		/* group the lines by their ids */
		final TupleTag<String> bertOutputTag = new TupleTag<>();
		final TupleTag<String> metadataTag = new TupleTag<>();
		PCollection<KV<String, CoGbkResult>> result = KeyedPCollectionTuple.of(bertOutputTag, idToBertOutputLines)
				.and(metadataTag, idToMetadataLines).apply(CoGroupByKey.create());

		final String dbUsername = options.getDbUsername();
		final String dbPassword = options.getDbPassword();
		final String databaseName = options.getDatabaseName();
		final String cloudSqlInstanceName = options.getMySqlInstanceName();
		final String projectId = options.getProject();
		final String cloudSqlRegion = options.getCloudSqlRegion();
		final BiolinkAssociation biolinkAssoc = options.getBiolinkAssociation();
//		final String idSuffixToProcess = options.getIdSuffixToProcess();

		PCollectionTuple sql = ClassifiedSentenceStorageSqlValuesFn.process(result, bertOutputTag, metadataTag,
				biolinkAssoc, bertScoreInclusionMinimumThreshold);

//		
//		
////		revise this to output a multi-pcollection - separate the evidence-score from the rest so that there is only one flow into evidence-score
//		PCollection<SqlValues> sqlValues = result.apply("compile sql values",
//				ParDo.of(new DoFn<KV<String, CoGbkResult>, SqlValues>() {
//					private static final long serialVersionUID = 1L;
//
//					@ProcessElement
//					public void processElement(ProcessContext c) {
//						KV<String, CoGbkResult> element = c.element();
//						CoGbkResult result = element.getValue();
//
//						/*
//						 * the result should have one line from each file except for the header in the
//						 * bert output file which won't have a corresponding match in the metadata file.
//						 * Below we get one line for each tag, check to make sure there aren't other
//						 * lines just in case, and handle the case of the bert output file header.
//						 */
//						String bertOutputLine = null;
//						Iterator<String> bertOutputLineIter = result.getAll(bertOutputTag).iterator();
//						if (bertOutputLineIter.hasNext()) {
//							bertOutputLine = bertOutputLineIter.next();
//							if (bertOutputLineIter.hasNext()) {
//								// if there is another line, check it just to make sure it's a redundant copy.
//								// If it's not identical, then error.
//								String nextBertOutputLine = bertOutputLineIter.next();
//								if (!nextBertOutputLine.equals(bertOutputLine)) {
//									throw new IllegalArgumentException(
//											"Did not expect another line to match from the BERT output file: "
//													+ bertOutputLine + " --- != --- " + nextBertOutputLine);
//								}
//							}
//						}
//
//						String metadataLine = null;
//						Iterator<String> metadataLineIter = result.getAll(metadataTag).iterator();
//						if (metadataLineIter.hasNext()) {
//							metadataLine = metadataLineIter.next();
//							if (metadataLineIter.hasNext()) {
//								// if there is another line, check it just to make sure it's a redundant copy.
//								// If it's not identical, then error.
//								//
//								// Note: this does happen -- there was a case of a sentence segment in both
//								// title and abstract -- document section probably needs to be used as part of
//								// the hash for creating the sentence id.
//
//								/*
//								 * e04d66197e09a4bcc57a0189add3d92d76ae641fac8a61d9b1ff6a675dd4e90f production
//								 * of the synthetic drug anti-@GENE$/@CHEMICAL$ using minicircle vector.
//								 * PMID:31266358 CD25 PR:000001380|PR:000012910 38|42 IL-10 DRUGBANK:DB12880
//								 * 43|48 73 production of the synthetic drug anti-CD25/IL-10 using minicircle
//								 * vector. title 2155 production of the synthetic drug anti-CD25/IL-10 using
//								 * minicircle vector.|||||||| production of the synthetic drug anti-CD25/IL-10
//								 * using minicircle vector. --- != ---
//								 * 
//								 * e04d66197e09a4bcc57a0189add3d92d76ae641fac8a61d9b1ff6a675dd4e90f production
//								 * of the synthetic drug anti-@GENE$/@CHEMICAL$ using minicircle vector.
//								 * PMID:31266358 CD25 PR:000001380|PR:000012910 38|42 IL-10 DRUGBANK:DB12880
//								 * 43|48 73 production of the synthetic drug anti-CD25/IL-10 using minicircle
//								 * vector. abstract 2155 production of the synthetic drug anti-CD25/IL-10 using
//								 * minicircle vector.|||||||| production of the synthetic drug anti-CD25/IL-10
//								 * using minicircle vector.
//								 * 
//								 */
//								String nextMetadataLine = metadataLineIter.next();
//								if (!nextMetadataLine.equals(metadataLine)) {
//									throw new IllegalArgumentException(
//											"Did not expect another line to match from the metadata file: "
//													+ metadataLine + " --- != --- " + nextMetadataLine);
//								}
//							}
//						}
//
//						/* both lines must have data in order to continue */
//						if (metadataLine != null && bertOutputLine != null) {
//
//							try {
//								int index = 0;
//								String[] bertOutputCols = bertOutputLine.split("\\t");
//								String sentenceId1 = bertOutputCols[index++];
//								@SuppressWarnings("unused")
//								String sentenceWithPlaceholders1 = bertOutputCols[index++];
//
//								// one of the scores for the predicates that is not
//								// BiolinkPredicate.NO_RELATION_PRESENT must be greater than the BERT minimum
//								// inclusion score threshold in order for this sentence to be considered
//								// evidence of an assertion. This is checked while populating the
//								// predicateCurietoScoreMap.
//								boolean hasScoreThatMeetsMinimumInclusionThreshold = false;
//								Map<String, Double> predicateCurieToScore = new HashMap<String, Double>();
//								for (String predicateCurie : getPredicateCuries(biolinkAssoc)) {
//									double score = Double.parseDouble(bertOutputCols[index++]);
//									predicateCurieToScore.put(predicateCurie, score);
//									if (!predicateCurie.equals("false") && score > bertScoreInclusionMinimumThreshold) {
//										hasScoreThatMeetsMinimumInclusionThreshold = true;
//									}
//
//								}
//
//								if (hasScoreThatMeetsMinimumInclusionThreshold) {
//									ExtractedSentence es = null;
//
//									try {
//										es = ExtractedSentence.fromTsv(metadataLine, true);
//									} catch (RuntimeException e) {
//										// some sentences in the PMCOA set contain tabs -- these need to be removed
//										// upstream. For now we catch the error so that processing can continue
//										es = null;
//									}
//									if (es != null) {
//
//										// if the IdSuffixToProcess is set then only process the sentence if it ends
//										// with that suffix
//										if (idSuffixToProcess.equals("null") || idSuffixToProcess.isEmpty()
//												|| idSuffixToProcess == null || (!idSuffixToProcess.isEmpty()
//														&& es.getDocumentId().endsWith(idSuffixToProcess))) {
//
//											// ensure the sentence identifer in the metadata line equals that from the
//											// bert
//											// output line. This check is probably unnecessary since the lines were
//											// keyed
//											// together based on the sentence ID initially, but we'll check just in
//											// case.
//											if (es.getSentenceIdentifier().equals(sentenceId1)) {
//												String subjectCoveredText;
//												String subjectCurie;
//												String subjectSpanStr;
//												String objectCoveredText;
//												String objectCurie;
//												String objectSpanStr;
//												if (es.getEntityPlaceholder1()
//														.equals(biolinkAssoc.getSubjectClass().getPlaceholder())) {
//													subjectCurie = es.getEntityId1();
//													subjectSpanStr = ExtractedSentence.getSpanStr(es.getEntitySpan1());
//													subjectCoveredText = es.getEntityCoveredText1();
//													objectCurie = es.getEntityId2();
//													objectSpanStr = ExtractedSentence.getSpanStr(es.getEntitySpan2());
//													objectCoveredText = es.getEntityCoveredText2();
//												} else {
//													subjectCurie = es.getEntityId2();
//													subjectSpanStr = ExtractedSentence.getSpanStr(es.getEntitySpan2());
//													subjectCoveredText = es.getEntityCoveredText2();
//													objectCurie = es.getEntityId1();
//													objectSpanStr = ExtractedSentence.getSpanStr(es.getEntitySpan1());
//													objectCoveredText = es.getEntityCoveredText1();
//												}
//
//												String documentId = es.getDocumentId();
//												String sentence = es.getSentenceText();
//
//												int documentYearPublished = es.getDocumentYearPublished();
//												if (documentYearPublished > 2155) {
//													// 2155 is the max year value in MySQL
//													documentYearPublished = 2155;
//												}
//
//												/*
//												 * for efficiency purposes, if there were multiple ontologies IDs
//												 * identified for the same span of text, those identifiers were spliced
//												 * together into a pipe-delimited list so that sentence only needed to
//												 * be classified once. Here we unsplice any spliced identifiers and
//												 * create separate sqlvalues for each.
//												 */
//												for (String sub : subjectCurie.split("\\|")) {
//													for (String obj : objectCurie.split("\\|")) {
//
//														String assertionId = DigestUtils
//																.sha256Hex(sub + obj + biolinkAssoc.getAssociationId());
//														String evidenceId = DigestUtils.sha256Hex(documentId + sentence
//																+ sub + subjectSpanStr + obj + objectSpanStr
//																+ biolinkAssoc.getAssociationId());
//														String subjectEntityId = DigestUtils
//																.sha256Hex(documentId + sentence + sub + subjectSpanStr
//																		+ biolinkAssoc.getAssociationId());
//														String objectEntityId = DigestUtils
//																.sha256Hex(documentId + sentence + obj + objectSpanStr
//																		+ biolinkAssoc.getAssociationId());
//
//														// make sure things will fit in the database columns
//
//														// sentences of this great length are most likely sentence
//														// segmentation
//														// errors.
//														// Here we truncate anything longer than 1900 characters so that
//														// it
//														// fits
//														// in
//														// the
//														// database column.
//														if (sentence.length() > 1900) {
//															sentence = sentence.substring(0, 1900);
//														}
//
//														if (sub.length() > 95) {
//															sub = sub.substring(0, 95);
//														}
//
//														if (obj.length() > 95) {
//															obj = obj.substring(0, 95);
//														}
//
//														String associationId = biolinkAssoc.getAssociationId();
//														if (associationId.length() > 95) {
//															associationId = associationId.substring(0, 95);
//														}
//
//														String documentZone = es.getDocumentZone();
//														if (documentZone.length() > 45) {
//															documentZone = documentZone.substring(0, 45);
//														}
//
//														String pubTypes = CollectionsUtil.createDelimitedString(
//																es.getDocumentPublicationTypes(), "|");
//														if (pubTypes.length() > 500) {
//															pubTypes = pubTypes.substring(0, 450);
//														}
//
//														if (subjectCoveredText.length() > 100) {
//															subjectCoveredText = subjectCoveredText.substring(0, 100);
//														}
//
//														if (objectCoveredText.length() > 100) {
//															objectCoveredText = objectCoveredText.substring(0, 100);
//														}
//
//														SqlValues sqlValues = new SqlValues(assertionId, sub, obj,
//																associationId, evidenceId, documentId, sentence,
//																subjectEntityId, objectEntityId, documentZone, pubTypes,
//																documentYearPublished, subjectSpanStr, objectSpanStr,
//																subjectCoveredText, objectCoveredText);
//
//														for (Entry<String, Double> entry : predicateCurieToScore
//																.entrySet()) {
//															String predicateCurie = entry.getKey();
//															if (predicateCurie.length() > 100) {
//																predicateCurie = predicateCurie.substring(0, 100);
//															}
//
//															sqlValues.addScore(predicateCurie, entry.getValue());
//														}
//
//														c.output(sqlValues);
//													}
//												}
//											} else {
//												throw new IllegalStateException(
//														"Mismatch between the BERT output file and sentence metadata files detected. "
//																+ "Sentence identifiers do not match!!! "
//																+ es.getSentenceIdentifier() + " != " + sentenceId1);
//											}
//										}
//									}
//								}
//							} catch (ArrayIndexOutOfBoundsException e) {
//								throw new IllegalArgumentException("ARRAY INDEX OOB on line: " + bertOutputLine, e);
//							}
//						}
//
//					}
//
//				}));

		// https://stackoverflow.com/questions/44699643/connecting-to-cloud-sql-from-dataflow-job
		// MYSQLINSTANCE format is project:zone:instancename.

		String instanceName = String.format("%s:%s:%s", projectId, cloudSqlRegion, cloudSqlInstanceName);
		String jdbcUrl = String.format(
				"jdbc:mysql://google/%s?cloudSqlInstance=%s&socketFactory=com.google.cloud.sql.mysql.SocketFactory&user=%s&password=%s&useUnicode=true&characterEncoding=UTF-8",
				databaseName, instanceName, dbUsername, dbPassword);

		DataSourceConfiguration dbConfig = JdbcIO.DataSourceConfiguration.create("com.mysql.cj.jdbc.Driver", jdbcUrl);

		int maxAttempts = 10;
		Duration maxDuration = Duration.millis(5000);
		Duration initialDuration = Duration.millis(5000);
		RetryConfiguration retryConfiguration = RetryConfiguration.create(maxAttempts, maxDuration, initialDuration);

//		PCollection<SqlValues> sqlValues = sql.get(ClassifiedSentenceStorageSqlValuesFn.SQLVALUES_OUTPUT_TAG);

		PCollection<EvidenceScoreTableValues> evidenceScoreTableValues = sql
				.get(ClassifiedSentenceStorageSqlValuesFn.EVIDENCE_SCORE_OUTPUT_TAG);

		PCollection<EntityTableValues> entityTableValues = sql
				.get(ClassifiedSentenceStorageSqlValuesFn.ENTITY_VALUES_OUTPUT_TAG);

		PCollection<AssertionTableValues> assertionTableValues = sql
				.get(ClassifiedSentenceStorageSqlValuesFn.ASSERTION_VALUES_OUTPUT_TAG);

		PCollection<EvidenceTableValues> evidenceTableValues = sql
				.get(ClassifiedSentenceStorageSqlValuesFn.EVIDENCE_VALUES_OUTPUT_TAG);

		/* Insert into assertions table - first, remove potential redundant records */

		PCollection<AssertionTableValues> uniqueAssertionTableValues = assertionTableValues
				.apply(Distinct.<AssertionTableValues>create()).setCoder(new AssertionTableValuesCoder());

		uniqueAssertionTableValues.apply("insert assertions", JdbcIO.<AssertionTableValues>write()
				.withDataSourceConfiguration(dbConfig).withRetryConfiguration(retryConfiguration)
				.withStatement("INSERT INTO assertion (assertion_id,subject_curie,object_curie,association_curie) \n"
						+ "values(?,?,?,?) ON DUPLICATE KEY UPDATE\n" + "    assertion_id = VALUES(assertion_id),\n"
						+ "    subject_curie = VALUES(subject_curie),\n" + "    object_curie = VALUES(object_curie),\n"
						+ "    association_curie = VALUES(association_curie)")
				.withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<AssertionTableValues>() {
					private static final long serialVersionUID = 1L;

					public void setParameters(AssertionTableValues assertionValues, PreparedStatement query)
							throws SQLException {
						query.setString(1, assertionValues.getAssertionId()); // assertion_id
						query.setString(2, assertionValues.getSubjectCurie()); // subject_curie
						query.setString(3, assertionValues.getObjectCurie()); // object_curie
						query.setString(4, assertionValues.getAssociationCurie()); // association_curie
					}
				}));

		/* Insert into evidence table */
		PCollection<EvidenceTableValues> uniqueEvidenceTableValues = evidenceTableValues
				.apply(Distinct.<EvidenceTableValues>create()).setCoder(new EvidenceTableValuesCoder());

		uniqueEvidenceTableValues.apply("insert evidence", JdbcIO.<EvidenceTableValues>write()
				.withDataSourceConfiguration(dbConfig).withRetryConfiguration(retryConfiguration)
				.withStatement(
						"INSERT INTO evidence (evidence_id, assertion_id, document_id, sentence, subject_entity_id, object_entity_id, document_zone, document_publication_type, document_year_published) \n"
								+ "values(?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE\n"
								+ "  evidence_id = VALUES(evidence_id),\n" + "  assertion_id = VALUES(assertion_id),\n"
								+ "  document_id = VALUES(document_id),\n" + "  sentence = VALUES(sentence),\n"
								+ "  subject_entity_id = VALUES(subject_entity_id),\n"
								+ "  object_entity_id = VALUES(object_entity_id),\n"
								+ "  document_zone = VALUES(document_zone),\n"
								+ "  document_publication_type = VALUES(document_publication_type),\n"
								+ "  document_year_published = VALUES(document_year_published)")
				.withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<EvidenceTableValues>() {
					private static final long serialVersionUID = 1L;

					public void setParameters(EvidenceTableValues evidenceValues, PreparedStatement query)
							throws SQLException {
						query.setString(1, evidenceValues.getEvidenceId()); // evidence_id
						query.setString(2, evidenceValues.getAssertionId());
						query.setString(3, evidenceValues.getDocumentId()); // document_id
						query.setString(4, evidenceValues.getSentence()); // sentence
						query.setString(5, evidenceValues.getSubjectEntityId()); // subject_entity_id
						query.setString(6, evidenceValues.getObjectEntityId()); // object_entity_id
						query.setString(7, evidenceValues.getDocumentZone()); // document_zone
						query.setString(8, evidenceValues.getDocumentPublicationTypesStr()); // document_publication_type
						query.setInt(9, evidenceValues.getDocumentYearPublished()); // document_year_published
					}
				}));

		/* Insert subject into entity table */
		PCollection<EntityTableValues> uniqueEntityTableValues = entityTableValues
				.apply(Distinct.<EntityTableValues>create()).setCoder(new EntityTableValuesCoder());
		uniqueEntityTableValues.apply("insert entity records",
				JdbcIO.<EntityTableValues>write().withDataSourceConfiguration(dbConfig)
						.withRetryConfiguration(retryConfiguration)
						.withStatement("INSERT INTO entity (entity_id, span, covered_text) \n"
								+ "values(?, ?, ?) ON DUPLICATE KEY UPDATE\n" + "  entity_id = VALUES(entity_id),\n"
								+ "  span = VALUES(span),\n" + "  covered_text = VALUES(covered_text)")
						.withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<EntityTableValues>() {
							private static final long serialVersionUID = 1L;

							public void setParameters(EntityTableValues entityValues, PreparedStatement query)
									throws SQLException {
								query.setString(1, entityValues.getEntityId()); // subject_entity_id
								query.setString(2, entityValues.getSpan()); // subject_span
								query.setString(3, entityValues.getCoveredText()); // subject_covered_text
							}
						}));

		// was previously inserting both subject and object entities simultaneously -
		// resulting in deadlocks (I think)
//		/* Insert object into entity table */
//		sqlValues.apply("insert object entity",
//				JdbcIO.<SqlValues>write().withDataSourceConfiguration(dbConfig)
//						.withRetryConfiguration(retryConfiguration)
//						.withStatement("INSERT INTO entity (entity_id, span, covered_text) \n"
//								+ "values(?, ?, ?) ON DUPLICATE KEY UPDATE\n" + "  entity_id = VALUES(entity_id),\n"
//								+ "  span = VALUES(span),\n" + "  covered_text = VALUES(covered_text)")
//						.withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<SqlValues>() {
//							private static final long serialVersionUID = 1L;
//
//							public void setParameters(SqlValues sqlValues, PreparedStatement query)
//									throws SQLException {
//								query.setString(1, sqlValues.getObjectEntityId()); // object_entity_id
//								query.setString(2, sqlValues.getObjectSpanStr()); // object_span
//								query.setString(3, sqlValues.getObjectCoveredText()); // object_covered_text
//							}
//						}));

		// commenting out old way of populating the evidence score table using sqlValues
		// - replaced with direct population using the EvidenceScoreTableValues
//		for (final String predicateCurie : getPredicateCuries(biolinkAssoc)) {
//			/* Insert into evidence score table */
//			sqlValues.apply("insert predicate=" + predicateCurie, JdbcIO.<SqlValues>write()
//					.withDataSourceConfiguration(dbConfig).withRetryConfiguration(retryConfiguration)
//					.withStatement("INSERT INTO evidence_score (evidence_id, predicate_curie, score) \n"
//							+ "values(?, ?, ?) ON DUPLICATE KEY UPDATE\n" + "  evidence_id = VALUES(evidence_id),\n"
//							+ "  predicate_curie = VALUES(predicate_curie),\n" + "  score = VALUES(score)")
//					.withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<SqlValues>() {
//						private static final long serialVersionUID = 1L;
//
//						public void setParameters(SqlValues sqlValues, PreparedStatement query) throws SQLException {
//
//							query.setString(1, sqlValues.getEvidenceId()); // evidence_id
//							query.setString(2, predicateCurie); // predicate_curie
//							query.setDouble(3, sqlValues.getScore(predicateCurie)); // score
//						}
//					}));
//		}

		/* Insert into evidence score table */
		PCollection<EvidenceScoreTableValues> uniqueEvidenceScoreTableValues = evidenceScoreTableValues
				.apply(Distinct.<EvidenceScoreTableValues>create()).setCoder(new EvidenceScoreTableValuesCoder());
		uniqueEvidenceScoreTableValues.apply("insert evidence scores",
				JdbcIO.<EvidenceScoreTableValues>write().withDataSourceConfiguration(dbConfig)
						.withRetryConfiguration(retryConfiguration)
						.withStatement("INSERT INTO evidence_score (evidence_id, predicate_curie, score) \n"
								+ "values(?, ?, ?) ON DUPLICATE KEY UPDATE\n" + "  evidence_id = VALUES(evidence_id),\n"
								+ "  predicate_curie = VALUES(predicate_curie),\n" + "  score = VALUES(score)")
						.withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<EvidenceScoreTableValues>() {
							private static final long serialVersionUID = 1L;

							public void setParameters(EvidenceScoreTableValues evtValues, PreparedStatement query)
									throws SQLException {

								query.setString(1, evtValues.getEvidenceId()); // evidence_id
								query.setString(2, evtValues.getPredicateCurie()); // predicate_curie
								query.setDouble(3, evtValues.getScore()); // score
							}
						}));

		p.run().waitUntilFinish();
	}

//	/**
//	 * @param idToSqlValues - unique ID to one of the SQL Table Values objects
//	 * @return a non-redundant collection of id to SQL table value pairings in hopes
//	 *         this will prevent deadlocks during DB inserts
//	 * 
//	 */
//	public static PCollection<KV<String, AssertionTableValues>> deduplicate(
//			PCollection<KV<String, AssertionTableValues>> idToValuesObject) {
//
//		PCollection<KV<String, Iterable<AssertionTableValues>>> idToRedundantObjects = idToValuesObject
//				.apply("group-by-id", GroupByKey.<String, AssertionTableValues>create());
//		PCollection<KV<String, AssertionTableValues>> nonredundantObjects = idToRedundantObjects.apply(
//				"deduplicate-by-id",
//				ParDo.of(new DoFn<KV<String, Iterable<AssertionTableValues>>, KV<String, AssertionTableValues>>() {
//					private static final long serialVersionUID = 1L;
//
//					@ProcessElement
//					public void processElement(ProcessContext c) {
//						Iterable<AssertionTableValues> texts = c.element().getValue();
//						String key = c.element().getKey();
//						// if there are more than one entity, we just return one
//						c.output(KV.of(key, texts.iterator().next()));
//					}
//				}));
//		return nonredundantObjects;
//	}

	/**
	 * @param lines
	 * @param keyColumn
	 * @return Given a PCollection of lines, return a PCollection<KV<String,
	 *         String>> where the key is the value of a specified column in the line
	 *         and the value is the line itself.
	 */
	private static PCollection<KV<String, String>> getKV(PCollection<String> lines, int keyColumn) {
		return lines.apply(ParDo.of(new DoFn<String, KV<String, String>>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				String line = c.element();
				String id = line.split("\\t")[keyColumn];
				c.output(KV.of(id, line));
			}
		}));
	}

}
