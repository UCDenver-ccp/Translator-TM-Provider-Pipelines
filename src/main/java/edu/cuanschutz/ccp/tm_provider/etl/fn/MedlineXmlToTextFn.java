package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.zip.GZIPInputStream;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.PipelineMain;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentCriteria;
import edu.cuanschutz.ccp.tm_provider.etl.util.ProcessingStatusFlag;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentWriter;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;
import lombok.Data;

/**
 * Outputs four {@link PCollection} objects
 * <ul>
 * <li>mapping document ID to plain text</li>
 * <li>mapping document ID to a serialized (BioNLP) form of the section
 * annotations -- in the case of Medline documents the sections are Title and
 * Abstract</li>
 * <li>a log of any failures</li>
 * <li>a status object that indicates which jobs still need processing, e.g.
 * dependency parse, etc.</li>
 * </ul>
 *
 */
public class MedlineXmlToTextFn extends DoFn<KV<String, String>, KV<String, String>> {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("serial")
	public static TupleTag<KV<String, List<String>>> plainTextTag = new TupleTag<KV<String, List<String>>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<KV<String, List<String>>> sectionAnnotationsTag = new TupleTag<KV<String, List<String>>>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<EtlFailureData> etlFailureTag = new TupleTag<EtlFailureData>() {
	};
	@SuppressWarnings("serial")
	public static TupleTag<ProcessingStatus> processingStatusTag = new TupleTag<ProcessingStatus>() {
	};

	public static PCollectionTuple process(PCollection<KV<String, String>> docIdToMedlineXml,
			DocumentCriteria outputTextDocCriteria, DocumentCriteria outputAnnotationDocCriteria,
			com.google.cloud.Timestamp timestamp, String collection) {

		return docIdToMedlineXml.apply("Convert Medline XML to plain text -- reserve section annotations",
				ParDo.of(new DoFn<KV<String, String>, KV<String, List<String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(@Element KV<String, String> docIdToMedlineXml, MultiOutputReceiver out) {
						String fileId = docIdToMedlineXml.getKey();
						String medlineXml = docIdToMedlineXml.getValue();

						try {
							SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
							InputStream is = new GZIPInputStream(new ByteArrayInputStream(medlineXml.getBytes()));
							SAXParser saxParser = saxParserFactory.newSAXParser();
							BeamExporterPlugin handlerPlugin = new BeamExporterPlugin(out, outputTextDocCriteria,
									outputAnnotationDocCriteria, collection);
							PubmedSaxHandler<BeamExporterPlugin> handler = new PubmedSaxHandler<BeamExporterPlugin>(handlerPlugin);
							saxParser.parse(is, handler);

						} catch (Throwable t) {
							EtlFailureData failure = new EtlFailureData(outputTextDocCriteria,
									"Likely failure during Medline XML parsing.", fileId, t, timestamp);
							out.get(etlFailureTag).output(failure);
						}

					}
				}).withOutputTags(plainTextTag,
						TupleTagList.of(sectionAnnotationsTag).and(etlFailureTag).and(processingStatusTag)));
	}

	private static class BeamExporterPlugin implements PubmedSaxHandlerPlugin {

		private final MultiOutputReceiver out;

		private final DocumentCriteria outputTextDocCriteria;

		private final DocumentCriteria outputAnnotationDocCriteria;

		private final String collection;

		public BeamExporterPlugin(MultiOutputReceiver out, DocumentCriteria outputTextDocCriteria,
				DocumentCriteria outputAnnotationDocCriteria, String collection) {
			this.out = out;
			this.outputTextDocCriteria = outputTextDocCriteria;
			this.outputAnnotationDocCriteria = outputAnnotationDocCriteria;
			this.collection = collection;
		}

		@Override
		public void handlePubmedDocument(TextDocument td) throws SAXException {
			String docId = td.getSourceid();
			String plainText = td.getText();

			/* serialize the annotations into the BioNLP format */
			BioNLPDocumentWriter bionlpWriter = new BioNLPDocumentWriter();
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			try {
				bionlpWriter.serialize(td, baos, CharacterEncoding.UTF_8);
				String serializedAnnotations = baos.toString(CharacterEncoding.UTF_8.getCharacterSetName());
				
				/*
				 * divide the document content into chunks if necessary so that each chunk is
				 * under the DataStore byte length threshold
				 */
				List<String> chunkedPlainText = PipelineMain.chunkContent(plainText);
				List<String> chunkedAnnotations = PipelineMain.chunkContent(serializedAnnotations);
				
				out.get(sectionAnnotationsTag).output(KV.of(docId, chunkedAnnotations));
				out.get(plainTextTag).output(KV.of(docId, chunkedPlainText));
				/*
				 * output a {@link ProcessingStatus} for the document
				 */
				ProcessingStatus status = new ProcessingStatus(docId);
				status.enableFlag(ProcessingStatusFlag.TEXT_DONE, outputTextDocCriteria, 1);
				status.enableFlag(ProcessingStatusFlag.SECTIONS_DONE, outputAnnotationDocCriteria, 1);

				if (collection != null) {
					status.addCollection(collection);
				}
				out.get(processingStatusTag).output(status);
			} catch (IOException e) {
				throw new SAXException(String.format(
						"Error while exporting document %s. Most likely a BioNLP format serialization issue.",
						td.getSourceid()), e);
			}

		}

	}

	static interface PubmedSaxHandlerPlugin {
		public void handlePubmedDocument(TextDocument td) throws SAXException;
	}

	static class PubmedSaxHandler<T extends PubmedSaxHandlerPlugin> extends DefaultHandler {

		private PubmedRecord record = null;

		private boolean inPmid;
		private boolean inTitle;
		private boolean inAbstract;

		private final T handlerPlugin;

		public PubmedSaxHandler(T handlerPlugin) {
			this.handlerPlugin = handlerPlugin;
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes)
				throws SAXException {

			if (qName.equalsIgnoreCase("MedlineCitation")) {
				if (record != null) {
					throw new IllegalStateException("record should be null");
				}
				record = new PubmedRecord();
			} else if (qName.equalsIgnoreCase("PMID")) {
				inPmid = true;
			} else if (qName.equalsIgnoreCase("ArticleTitle")) {
				inTitle = true;
			} else if (qName.equalsIgnoreCase("AbstractText")) {
				inAbstract = true;
				// adds a line break unless this is the first abstract text encountered
				record.addAbstractLineBreak();
			}

		}

		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {
			if (qName.equalsIgnoreCase("MedlineCitation")) {
				String documentText = (record.getAbstractText().isEmpty()) ? record.getTitle()
						: String.format("%s\n\n%s", record.getTitle(), record.getAbstractText());

				TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults(record.getPmid());
				TextAnnotation titleAnnotation = factory.createAnnotation(0, record.getTitle().length(),
						record.getTitle(), "title");
				TextAnnotation abstractAnnotation = null;
				if (!record.getAbstractText().isEmpty()) {
					int abstractStart = record.getTitle().length() + 2;
					int abstractEnd = abstractStart + record.getAbstractText().length();
					abstractAnnotation = factory.createAnnotation(abstractStart, abstractEnd, record.getAbstractText(),
							"abstract");
				}

				TextDocument td = new TextDocument(record.getPmid(), "PubMed", documentText);
				td.addAnnotation(titleAnnotation);
				if (abstractAnnotation != null) {
					td.addAnnotation(abstractAnnotation);
				}

				handlerPlugin.handlePubmedDocument(td);
				record = null;
			} else if (qName.equalsIgnoreCase("PMID")) {
				inPmid = false;
			} else if (qName.equalsIgnoreCase("ArticleTitle")) {
				inTitle = false;
			} else if (qName.equalsIgnoreCase("AbstractText")) {
				inAbstract = false;
			}
		}

		@Override
		public void characters(char ch[], int start, int length) throws SAXException {
			if (inPmid) {
				record.setPmid(new String(ch, start, length));
			} else if (inTitle) {
				record.setTitle(new String(ch, start, length));
			} else if (inAbstract) {
				String text = new String(ch, start, length);
				record.addAbstractText(text);
			}
		}

	}

	@Data
	static class PubmedRecord {
		private String pmid;
		private String title;
		private StringBuilder abstractText = new StringBuilder();

		public void addAbstractText(String text) {
				abstractText.append(text);
		}
		
		public void addAbstractLineBreak() {
			if (abstractText.length() > 0) {
				abstractText.append("\n");
			}
		}
		
		public String getAbstractText() {
			return abstractText.toString();
		}
	}

}
