package edu.cuanschutz.ccp.tm_provider.etl;

import static com.google.datastore.v1.client.DatastoreHelper.makeFilter;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_KIND;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_DOCUMENT_ID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.google.datastore.v1.CompositeFilter;
import com.google.datastore.v1.CompositeFilter.Operator;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Filter;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.Value;

import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreDocumentUtil;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentFormat;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.cuanschutz.ccp.tm_provider.etl.util.Version;

public class PipelineMain {
	public static void main(String[] args) {
		System.out.println("Running pipeline version: " + Version.getProjectVersion());
		PipelineKey pipeline = null;
		try {
			pipeline = PipelineKey.valueOf(args[0]);
		} catch (IllegalArgumentException e) {
			System.err.println(String.format("Unrecognized pipeline (%s). Valid choices are %s", args[0],
					Arrays.asList(PipelineKey.values()).stream().map(k -> k.name()).collect(Collectors.toList())));
			pipeline = null;
		}
		if (pipeline != null) {
			String[] pipelineArgs = Arrays.copyOfRange(args, 1, args.length);
			switch (pipeline) {
			case BIOC_TO_TEXT:
				BiocToTextPipeline.main(pipelineArgs);
				break;
			case DEPENDENCY_PARSE:
				DependencyParsePipeline.main(pipelineArgs);
				break;
			case OGER:
				OgerPipeline.main(pipelineArgs);
				break;
			default:
				throw new IllegalArgumentException(String.format(
						"Valid pipeline (%s) but a code change required before it can be used. Valid choices are %s",
						args[0],
						Arrays.asList(PipelineKey.values()).stream().map(k -> k.name()).collect(Collectors.toList())));
			}
		}
	}

	public static PCollection<KV<String, String>> getDocId2Content(String pipelineVersion, String project, Pipeline p,
			ProcessingStatusFlag targetProcessStatusFlag, Set<ProcessingStatusFlag> requiredProcessStatusFlags) {
		PCollection<Entity> status = getStatusEntitiesToProcess(p, targetProcessStatusFlag, requiredProcessStatusFlags,
				project);

		PCollection<KV<String, String>> docId2Content = status.apply("get document content",
				ParDo.of(new DoFn<Entity, KV<String, String>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(@Element Entity status, OutputReceiver<KV<String, String>> out) {
						Value value = status.getPropertiesMap().get(STATUS_PROPERTY_DOCUMENT_ID);
						String documentId = value.getStringValue();

						DatastoreDocumentUtil util = new DatastoreDocumentUtil();
						KV<String, String> documentIdToContent = util.getDocumentIdToContent(documentId,
								DocumentType.TEXT, DocumentFormat.TEXT, PipelineKey.BIOC_TO_TEXT, pipelineVersion);
						if (documentIdToContent != null) {
							out.output(documentIdToContent);
						}

					}

				}));
		return docId2Content;
	}

	public static PCollection<Entity> getStatusEntitiesToProcess(Pipeline p,
			ProcessingStatusFlag targetProcessStatusFlag, Set<ProcessingStatusFlag> requiredProcessStatusFlags,
			String project) {
		List<Filter> filters = new ArrayList<Filter>();
		for (ProcessingStatusFlag flag : requiredProcessStatusFlags) {
			Filter filter = makeFilter(flag.getDatastorePropertyName(), PropertyFilter.Operator.EQUAL, makeValue(true))
					.build();
			filters.add(filter);
		}
		filters.add(makeFilter(targetProcessStatusFlag.getDatastorePropertyName(), PropertyFilter.Operator.EQUAL,
				makeValue(false)).build());

		Query.Builder query = Query.newBuilder();
		query.addKindBuilder().setName(STATUS_KIND);

		CompositeFilter.Builder compositeFilter = CompositeFilter.newBuilder();
		compositeFilter.addAllFilters(filters);
		compositeFilter.setOp(Operator.AND);
		Filter filter = Filter.newBuilder().setCompositeFilter(compositeFilter).build();
		query.setFilter(filter);

		PCollection<Entity> status = p.apply("document_entity->datastore",
				DatastoreIO.v1().read().withQuery(query.build()).withProjectId(project));
		return status;
	}

}
