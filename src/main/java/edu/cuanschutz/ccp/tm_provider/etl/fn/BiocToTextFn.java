package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import edu.cuanschutz.ccp.tm_provider.etl.util.BiocToTextConverter;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentWriter;

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

	public static PCollectionTuple process(PCollection<KV<String, String>> docIdToBiocXml) {

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
							}
						} catch (Throwable t) {
							EtlFailureData failure = new EtlFailureData("Likely failure during BioC XML parsing.",
									fileId, biocXml, t);
							out.get(etlFailureTag).output(failure);
						}

					}
				}).withOutputTags(plainTextTag, TupleTagList.of(sectionAnnotationsTag).and(etlFailureTag)));
	}

}
