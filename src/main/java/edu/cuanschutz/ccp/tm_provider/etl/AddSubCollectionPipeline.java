package edu.cuanschutz.ccp.tm_provider.etl;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.tools.ant.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.google.datastore.v1.client.DatastoreHelper;

import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;

/**
 * This Apache Beam pipeline processes documents with the OGER concept
 * recognition service reached via HTTP POST. Input is plain text; Output is
 * concept annotations in BioNLP format.
 */
public class AddSubCollectionPipeline {

	private final static Logger LOGGER = Logger.getLogger(AddSubCollectionPipeline.class.getName());

	public interface Options extends DataflowPipelineOptions {
		@Description("The document collection to process")
		String getCollection();

		void setCollection(String value);

		@Description("The document collection to process")
		String getSubCollectionPrefix();

		void setSubCollectionPrefix(String value);

	}

	public static void main(String[] args) {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		Pipeline p = Pipeline.create(options);

		// require that the documents have a plain text version to be processed by OGER
		Set<ProcessingStatusFlag> requiredProcessStatusFlags = EnumSet.of(ProcessingStatusFlag.TEXT_DONE);

		PCollection<Entity> statusEntities = PipelineMain.getStatusEntitiesToProcess(p, ProcessingStatusFlag.NOOP,
				requiredProcessStatusFlags, options.getProject(), options.getCollection(), OverwriteOutput.YES, 0);

		String subCollectionPrefix = options.getSubCollectionPrefix();

		PCollection<Entity> updatedEntities = statusEntities.apply("add sub collection...",
				ParDo.of(new DoFn<Entity, Entity>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c, BoundedWindow window) {
						Entity updatedEntity = updateStatusEntity(c.element(), subCollectionPrefix);
						c.output(updatedEntity);
					}
				}));

		PCollection<Entity> nonredundantStatusEntities = PipelineMain.deduplicateStatusEntities(updatedEntities);
		nonredundantStatusEntities.apply("status_entity->datastore",
				DatastoreIO.v1().write().withProjectId(options.getProject()));

		p.run().waitUntilFinish();
	}

	@VisibleForTesting
	protected static Entity updateStatusEntity(Entity origEntity, String subCollectionPrefix) {
		Key key = origEntity.getKey();

		Entity.Builder entityBuilder = Entity.newBuilder();
		entityBuilder.setKey(key);

		String documentId = origEntity.getPropertiesMap().get(DatastoreConstants.STATUS_PROPERTY_DOCUMENT_ID)
				.getStringValue();

		for (Entry<String, Value> entry : origEntity.getPropertiesMap().entrySet()) {
			if (entry.getKey().equals(DatastoreConstants.STATUS_PROPERTY_COLLECTIONS)) {
				List<Value> collectionNames = DatastoreHelper.getList(entry.getValue());

				List<Value> updatedCollectionNames = new ArrayList<Value>();
				// ignore exising subcollections
				for (Value v : collectionNames) {
					if (!v.getStringValue().startsWith(subCollectionPrefix)) {
						updatedCollectionNames.add(v);
					}
				}
				String collectionNameToAdd = createSubCollectionName(subCollectionPrefix, documentId);
				if (collectionNameToAdd != null) {
					updatedCollectionNames.add(DatastoreHelper.makeValue(collectionNameToAdd).build());
				}

				entityBuilder.putProperties(entry.getKey(), DatastoreHelper.makeValue(updatedCollectionNames).build());
			} else {
				entityBuilder.putProperties(entry.getKey(), entry.getValue());
			}
		}

		Entity entity = entityBuilder.build();
		return entity;
	}

	/**
	 * Hardcoded to assume PubMed identifier. The sub collection number is based on
	 * the PMID.
	 * 
	 * @param subCollectionPrefix
	 * @param documentId
	 * @return
	 */
	@VisibleForTesting
	protected static String createSubCollectionName(String subCollectionPrefix, String documentId) {
		if (documentId.startsWith("PMID:")) {
			int pmid = Integer.parseInt(StringUtils.removePrefix(documentId, "PMID:"));

			int windowSize = 1000000;

			int subCollectionIndex = 0;

			while (true) {
				int windowStart = subCollectionIndex * windowSize;
				int windowEnd = windowStart + windowSize;
				if (pmid >= windowStart && pmid < windowEnd) {
					return String.format("%s%d", subCollectionPrefix, subCollectionIndex);
				}
				subCollectionIndex++;
			}
		}
		return null;

	}

}
