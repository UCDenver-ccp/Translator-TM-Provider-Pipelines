package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.EnumSet;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.util.BiocToTextConverter;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
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
	@SuppressWarnings("serial")
	public static TupleTag<KV<String, String>> plainTextTag = new TupleTag<KV<String, String>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<KV<String, String>> sectionAnnotationsTag = new TupleTag<KV<String, String>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> etlFailureTag = new TupleTag<EtlFailureData>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<ProcessingStatus> processingStatusTag = new TupleTag<ProcessingStatus>() {
	};

	public static PCollectionTuple process(PCollection<KV<String, String>> docIdToBiocXml, PipelineKey pipeline,
			String pipelineVersion, com.google.cloud.Timestamp timestamp) {

		return docIdToBiocXml.apply("Convert BioC XML to plain text -- reserve section annotations",
				ParDo.of(new DoFn<KV<String, String>, KV<String, String>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(@Element KV<String, String> docIdToBioc, MultiOutputReceiver out) {
						String fileId = docIdToBioc.getKey();
						String biocXml = docIdToBioc.getValue();

						try {
							Map<String, TextDocument> docIdToDocumentMap = BiocToTextConverter
									.convert(new ByteArrayInputStream(biocXml.getBytes()));

							/*
							 * It's possible that there are multiple documents in the map, but there is
							 * likely only one document in the map.
							 */
							for (Entry<String, TextDocument> entry : docIdToDocumentMap.entrySet()) {
								String docId = entry.getKey();
								String plainText = entry.getValue().getText();
								out.get(plainTextTag).output(KV.of(docId, plainText));

								/* serialize the annotations into the BioNLP format */
								BioNLPDocumentWriter bionlpWriter = new BioNLPDocumentWriter();
								ByteArrayOutputStream baos = new ByteArrayOutputStream();
								bionlpWriter.serialize(entry.getValue(), baos, CharacterEncoding.UTF_8);
								String serializedAnnotations = baos
										.toString(CharacterEncoding.UTF_8.getCharacterSetName());
								out.get(sectionAnnotationsTag).output(KV.of(docId, serializedAnnotations));

								/*
								 * output a {@link ProcessingStatus} for the document
								 */
								ProcessingStatus status = new ProcessingStatus(docId,
										EnumSet.of(ProcessingStatusFlag.TEXT_DONE));
								out.get(processingStatusTag).output(status);

							}
						} catch (Throwable t) {
							EtlFailureData failure = new EtlFailureData(pipeline, pipelineVersion,
									"Likely failure during BioC XML parsing.", fileId, t, timestamp);
							out.get(etlFailureTag).output(failure);
						}

					}
				}).withOutputTags(plainTextTag,
						TupleTagList.of(sectionAnnotationsTag).and(etlFailureTag).and(processingStatusTag)));
	}

}
