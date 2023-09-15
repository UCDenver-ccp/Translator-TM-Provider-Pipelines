package edu.cuanschutz.ccp.tm_provider.etl;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import com.spotify.scio.transforms.RateLimiterDoFn;

import edu.cuanschutz.ccp.tm_provider.etl.ConceptCooccurrenceMetricsPipeline.ConceptId;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ConceptCooccurrenceCountsFn;
import edu.cuanschutz.ccp.tm_provider.etl.fn.ConceptCooccurrenceCountsFn.CooccurLevel;
import edu.cuanschutz.ccp.tm_provider.etl.fn.PCollectionUtil;
import edu.cuanschutz.ccp.tm_provider.etl.fn.PCollectionUtil.Delimiter;

/**
 * Load files containing concept counts from the
 * {@link ConceptCooccurrenceCountsPipelineTest} and computes the inverse
 * document frequency for each concept.
 */
public class ConceptIdfPipeline {

	public interface Options extends DataflowPipelineOptions {

		@Description("Path to the bucket where the count files are located")
		@Required
		String getCountFileBucket();

		void setCountFileBucket(String bucketPath);

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

		/* Inverse document frequency should be calculated at the document level */
		final CooccurLevel level = CooccurLevel.DOCUMENT;

		String docIdToConceptIdFilePattern = options.getCountFileBucket() + "/"
				+ ConceptCooccurrenceCountsPipeline.getDocumentIdToConceptIdsFileNamePrefix(level) + "*";

		PCollection<KV<String, Set<String>>> textIdToConceptIdCollection = PCollectionUtil.fromKeyToSetTwoColumnFiles(
				"load docid to conceptid - " + level.name().toLowerCase(), p, docIdToConceptIdFilePattern,
				ConceptCooccurrenceCountsFn.OUTPUT_FILE_COLUMN_DELIMITER,
				ConceptCooccurrenceCountsFn.OUTPUT_FILE_SET_DELIMITER, Compression.UNCOMPRESSED);

		// then supplement each concept id set with all ancestor ids
		boolean addAncestors = true;
		PCollection<KV<String, Set<ConceptId>>> textIdToConceptIdWithAncestorsCollection = ConceptCooccurrenceMetricsPipeline
				.addAncestorConceptIds(p, level, textIdToConceptIdCollection, addAncestors, ancestorMapView);

		// create a mapping from each concept ID to the number of documents in which it
		// was observed, then convert it into a View
		final PCollection<KV<String, Long>> conceptIdToCounts = ConceptCooccurrenceMetricsPipeline
				.countConceptObservations(p, level, textIdToConceptIdWithAncestorsCollection);

		// calculate the total number of documents that were processed
		final PCollectionView<Long> totalDocumentCount = ConceptCooccurrenceMetricsPipeline.countTotalDocumentsView(p,
				level, textIdToConceptIdWithAncestorsCollection);

		/* compute the inverse document frequency for all concepts */
		PCollection<KV<String, Double>> conceptIdToIdf = getConceptIdf(totalDocumentCount, conceptIdToCounts, level);

		/* ---- INSERT INTO DATABASE BELOW ---- */

		final double recordsPerSecond = 14.5;

		/* Insert into concept_idf table */
		conceptIdToIdf.apply(ParDo.of(new RateLimiterDoFn<>(recordsPerSecond))).apply(
				"insert concept_idf - " + level.name().toLowerCase(),
				JdbcIO.<KV<String, Double>>write().withDataSourceConfiguration(dbConfig)
						.withStatement("INSERT INTO concept_idf (concept_curie,level,idf) \n"
						// @formatter:off
							+ "values(?,?,?) ON DUPLICATE KEY UPDATE\n" 
							+ "    concept_curie = VALUES(concept_curie),\n"
							+ "    level = VALUES(level),\n"
							+ "    idf = VALUES(idf)")
							// @formatter:on
						.withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<KV<String, Double>>() {
							private static final long serialVersionUID = 1L;

							public void setParameters(KV<String, Double> conceptIdToIdf, PreparedStatement query)
									throws SQLException {
								query.setString(1, conceptIdToIdf.getKey());
								query.setString(2, level.name().toLowerCase());
								query.setDouble(3, conceptIdToIdf.getValue());
							}
						}).withResults());

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

						BigDecimal d = new BigDecimal(idf).setScale(8, BigDecimal.ROUND_HALF_UP);

						c.output(KV.of(conceptId, d.doubleValue()));
					}
				}).withSideInputs(totalDocumentCountView));
	}

}
