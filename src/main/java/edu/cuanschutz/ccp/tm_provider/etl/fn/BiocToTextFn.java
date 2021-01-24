package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.util.BiocToTextConverter;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreProcessingStatusUtil.OverwriteOutput;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentWriter;

/**
 * Outputs four {@link PCollection} objects
 * <ul>
 * <li>mapping document ID to plain text</li>
 * <li>mapping document ID to a serialized (BioNLP) form of the section
 * annotations</li>
 * <li>a log of any failures</li>
 * <li>a status object that indicates which jobs still need processing, e.g.
 * dependency parse, etc.</li>
 * </ul>
 *
 */
public class BiocToTextFn extends DoFn<KV<String, String>, KV<String, String>> {

	private static final long serialVersionUID = 1L;
	/**
	 * The value in the returned KV pair is a list because it is possible that the
	 * whole document content string is too large to store in Datastore. The list
	 * allows it to be stored in chunks.
	 */
	@SuppressWarnings("serial")
	public static TupleTag<KV<String, List<String>>> plainTextTag = new TupleTag<KV<String, List<String>>>() {
	};
	/**
	 * The value in the returned KV pair is a list because it is possible that the
	 * whole document content string is too large to store in Datastore. The list
	 * allows it to be stored in chunks.
	 */
	@SuppressWarnings("serial")
	public static TupleTag<KV<String, List<String>>> sectionAnnotationsTag = new TupleTag<KV<String, List<String>>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> etlFailureTag = new TupleTag<EtlFailureData>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<ProcessingStatus> processingStatusTag = new TupleTag<ProcessingStatus>() {
	};

	public static PCollectionTuple process(PCollection<KV<String, String>> docIdToBiocXml,
			DocumentCriteria outputTextDocCriteria, DocumentCriteria outputAnnotationDocCriteria,
			com.google.cloud.Timestamp timestamp, String collection, PCollectionView<Set<String>> existingDocumentIds,
			OverwriteOutput overwriteOutput) {

		return docIdToBiocXml.apply("Convert BioC XML to plain text -- reserve section annotations",
				ParDo.of(new DoFn<KV<String, String>, KV<String, List<String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext context) {

						KV<String, String> docIdToBioc = context.element();

						String fileId = docIdToBioc.getKey();
						String biocXml = docIdToBioc.getValue();

						try {
							Map<String, TextDocument> docIdToDocumentMap = BiocToTextConverter
									.convert(new ByteArrayInputStream(biocXml.getBytes()));

							Set<String> alreadyStoredDocIds = context.sideInput(existingDocumentIds);
							/*
							 * It's possible that there are multiple documents in the map, but there is
							 * likely only one document in the map.
							 */
							for (Entry<String, TextDocument> entry : docIdToDocumentMap.entrySet()) {
								String docId = entry.getKey();

								// if the document id has already been stored, then don't store it again
								if (overwriteOutput == OverwriteOutput.NO && alreadyStoredDocIds.contains(docId)) {
									continue;
								}
								String plainText = entry.getValue().getText();

								/*
								 * divide the document content into chunks if necessary so that each chunk is
								 * under the DataStore byte length threshold
								 */
								List<String> chunkedPlainText = PipelineMain.chunkContent(plainText);

								/* serialize the annotations into the BioNLP format */
								BioNLPDocumentWriter bionlpWriter = new BioNLPDocumentWriter();
								ByteArrayOutputStream baos = new ByteArrayOutputStream();
								bionlpWriter.serialize(entry.getValue(), baos, CharacterEncoding.UTF_8);
								String serializedAnnotations = baos
										.toString(CharacterEncoding.UTF_8.getCharacterSetName());

								List<String> chunkedAnnotations = PipelineMain.chunkContent(serializedAnnotations);

								context.output(sectionAnnotationsTag, KV.of(docId, chunkedAnnotations));
								context.output(plainTextTag, KV.of(docId, chunkedPlainText));
								/*
								 * output a {@link ProcessingStatus} for the document
								 */
								ProcessingStatus status = new ProcessingStatus(docId);
								status.enableFlag(ProcessingStatusFlag.TEXT_DONE);
								status.enableFlag(ProcessingStatusFlag.SECTIONS_DONE);

								if (collection != null) {
									status.addCollection(collection);
								}
								context.output(processingStatusTag, status);

							}
						} catch (Throwable t) {
							EtlFailureData failure = new EtlFailureData(outputTextDocCriteria,
									"Likely failure during BioC XML parsing.", fileId, t, timestamp);
							context.output(etlFailureTag, failure);
						}

					}
				}).withSideInputs(existingDocumentIds).withOutputTags(plainTextTag,
						TupleTagList.of(sectionAnnotationsTag).and(etlFailureTag).and(processingStatusTag)));
	}

}
