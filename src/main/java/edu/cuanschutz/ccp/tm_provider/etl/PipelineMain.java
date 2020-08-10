package edu.cuanschutz.ccp.tm_provider.etl;

import static com.google.datastore.v1.client.DatastoreHelper.makeAndFilter;
import static com.google.datastore.v1.client.DatastoreHelper.makeFilter;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_KIND;
import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.STATUS_PROPERTY_DOCUMENT_ID;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Filter;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.Query;

import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreDocumentUtil;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.cuanschutz.ccp.tm_provider.etl.util.Version;
import edu.ucdenver.ccp.common.file.CharacterEncoding;

public class PipelineMain {

	private final static Logger LOGGER = Logger.getLogger(PipelineMain.class.getName());

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
			case FILE_LOAD:
				LoadFilesPipeline.main(pipelineArgs);
				break;
			case OGER:
				OgerPipeline.main(pipelineArgs);
				break;
			case BIGQUERY_EXPORT:
				BigQueryExportPipeline.main(pipelineArgs);
				break;
			case SENTENCE_SEGMENTATION:
				SentenceSegmentationPipeline.main(pipelineArgs);
				break;
			case SENTENCE_COOCCURRENCE_EXPORT:
				SentenceCooccurrencePipeline.main(pipelineArgs);
				break;
			case DRY_RUN:
				DryRunPipeline.main(pipelineArgs);
				break;
			default:
				throw new IllegalArgumentException(String.format(
						"Valid pipeline (%s) but a code change required before it can be used. Valid choices are %s",
						args[0],
						Arrays.asList(PipelineKey.values()).stream().map(k -> k.name()).collect(Collectors.toList())));
			}
		}
	}

	public static PCollection<KV<String, String>> getDocId2Content(DocumentCriteria inputDocCriteria,
//			String pipelineVersion, 
			String gcpProjectId, Pipeline beamPipeline, ProcessingStatusFlag targetProcessStatusFlag,
			Set<ProcessingStatusFlag> requiredProcessStatusFlags, String collection, OverwriteOutput overwriteOutput) {

		/*
		 * get the status entities for documents that meet the required process status
		 * flag critera but whose target process status flag is false
		 */
		PCollection<Entity> status = getStatusEntitiesToProcess(beamPipeline, targetProcessStatusFlag,
				requiredProcessStatusFlags, gcpProjectId, collection, overwriteOutput);

		/*
		 * then return a mapping from document id to the document content that will be
		 * processed
		 */
		PCollection<KV<String, String>> docId2Content = status.apply("get document content",
				ParDo.of(new DoFn<Entity, KV<String, String>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(@Element Entity status, OutputReceiver<KV<String, String>> out) {
						String documentId = status.getPropertiesMap().get(STATUS_PROPERTY_DOCUMENT_ID).getStringValue();

						// TODO: This needs to be fixed -- this chunk count field is only present for
						// the text document right now
//						String chunkCountPropertyName = DatastoreProcessingStatusUtil
//								.getDocumentChunkCountPropertyName(inputDocCriteria);
//						long chunkCount = status.getPropertiesMap().get(chunkCountPropertyName).getIntegerValue();

						long chunkCount = 7; // this is the max chunk count in the CORD 19 data

						DatastoreDocumentUtil util = new DatastoreDocumentUtil();
						KV<String, String> documentIdToContent = util.getDocumentIdToContent(documentId,
								inputDocCriteria, chunkCount);
						if (documentIdToContent != null) {
							out.output(documentIdToContent);
						}
					}
				}));
		return docId2Content;
	}

	public static PCollection<Entity> getStatusEntitiesToProcess(Pipeline p,
			ProcessingStatusFlag targetProcessStatusFlag, Set<ProcessingStatusFlag> requiredProcessStatusFlags,
			String gcpProjectId, String collection, OverwriteOutput overwriteOutput) {
		List<Filter> filters = new ArrayList<Filter>();
		for (ProcessingStatusFlag flag : requiredProcessStatusFlags) {
			Filter filter = makeFilter(flag.getDatastoreFlagPropertyName(), PropertyFilter.Operator.EQUAL,
					makeValue(true)).build();
			filters.add(filter);
		}

		if (overwriteOutput == OverwriteOutput.NO) {
			filters.add(makeFilter(targetProcessStatusFlag.getDatastoreFlagPropertyName(),
					PropertyFilter.Operator.EQUAL, makeValue(false)).build());
		}

		/* incorporate the collection name into the query if there is one */
		if (collection != null) {
			filters.add(makeFilter(DatastoreConstants.STATUS_PROPERTY_COLLECTIONS, PropertyFilter.Operator.EQUAL,
					makeValue(collection)).build());
		}

		for (Filter filter : filters) {
			LOGGER.log(Level.INFO, "FILTER: " + filter.toString());
		}

		Filter filter = makeAndFilter(filters).build();
		Query.Builder query = Query.newBuilder();
		query.addKindBuilder().setName(STATUS_KIND);
		query.setFilter(filter);

		PCollection<Entity> status = p.apply("datastore->status entities to process",
				DatastoreIO.v1().read().withQuery(query.build()).withProjectId(gcpProjectId));
		return status;
	}

	/**
	 * @param input
	 * @return the input string divided (if necessary) into chunks small enough to
	 *         fit under the max byte threshold imposed by DataStore
	 * @throws UnsupportedEncodingException
	 */
	public static List<String> chunkContent(String input) throws UnsupportedEncodingException {
		List<String> chunks = new ArrayList<String>();

		if (input.getBytes("UTF-8").length < DatastoreConstants.MAX_STRING_STORAGE_SIZE_IN_BYTES) {
			chunks.add(input);
		} else {
			chunks.addAll(splitStringByByteLength(input, CharacterEncoding.UTF_8,
					DatastoreConstants.MAX_STRING_STORAGE_SIZE_IN_BYTES));
		}

		return chunks;
	}

	/**
	 * from:
	 * https://stackoverflow.com/questions/48868721/splitting-a-string-with-byte-length-limits-in-java
	 * 
	 * @param src
	 * @param encoding
	 * @param maxsize
	 * @return
	 */
	public static List<String> splitStringByByteLength(String src, CharacterEncoding encoding, int maxsize) {
		Charset cs = Charset.forName(encoding.getCharacterSetName());
		CharsetEncoder coder = cs.newEncoder();
		ByteBuffer out = ByteBuffer.allocate(maxsize); // output buffer of required size
		CharBuffer in = CharBuffer.wrap(src);
		List<String> ss = new ArrayList<>(); // a list to store the chunks
		int pos = 0;
		while (true) {
			CoderResult cr = coder.encode(in, out, true); // try to encode as much as possible
			int newpos = src.length() - in.length();
			String s = src.substring(pos, newpos);
			ss.add(s); // add what has been encoded to the list
			pos = newpos; // store new input position
			out.rewind(); // and rewind output buffer
			if (!cr.isOverflow()) {
				break; // everything has been encoded
			}
		}
		return ss;
	}

}
