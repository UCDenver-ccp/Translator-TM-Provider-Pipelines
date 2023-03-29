package edu.cuanschutz.ccp.tm_provider.relation_extraction;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentWriter;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;
import lombok.Data;

/**
 * Originally designed to convert the output of a BERT NER model (which is
 * CoNLL-like) to the BioNLP format used to capture entities in the rest of this
 * project.
 * 
 * In order to do this, the process consumes 2 files: 1) the BERT input file
 * (b/c it contains document IDs and span information) 2) the BERT output file
 * which contains the predicted labels for the entities
 * 
 * 
 * Output: a Map linking the document ID to sa String representation in BioNLP
 * format of all entity annotations for a document
 */
public class NerBertToBionlpConverter {

	public static void convert(File bertInputFile, File bertOutputFile, File outputDir)
			throws FileNotFoundException, IOException {

		BioNLPDocumentWriter docWriter = new BioNLPDocumentWriter();

		for (EntityDocumentIterator docIter = new EntityDocumentIterator(new FileInputStream(bertInputFile),
				new FileInputStream(bertOutputFile)); docIter.hasNext();) {

			TextDocument td = docIter.next();
			File outputFile = new File(outputDir, td.getSourceid() + ".bionlp");
			docWriter.serialize(td, outputFile, CharacterEncoding.UTF_8);
		}
	}

	public static class EntityDocumentIterator implements Iterator<TextDocument> {

		private TextDocument nextDocument = null;
		private StreamLineIterator bertInputLineIter;
		private StreamLineIterator bertOutputLineIter;
		private String awaitingInputLine = null;
		private String awaitingOutputLine = null;

		public enum BertStreamType {
			INPUT, OUTPUT
		}

		public EntityDocumentIterator(InputStream bertInputStream, InputStream bertOutputStream) throws IOException {
			bertInputLineIter = new StreamLineIterator(bertInputStream, CharacterEncoding.UTF_8, null);
			bertOutputLineIter = new StreamLineIterator(bertOutputStream, CharacterEncoding.UTF_8, null);
			nextDocument = getNextDocument();
		}

		private TextDocument getNextDocument() {
			TextDocument td = null;
			List<TextAnnotation> entityAnnots = new ArrayList<TextAnnotation>();
			String documentId = (awaitingInputLine != null) ? awaitingInputLine.split("\\t")[1] : null;

			String lastTag = "O";

			if (documentId == null) {
				// this will only happen for the very first document
				awaitingInputLine = fastForward(bertInputLineIter);
				awaitingOutputLine = fastForward(bertOutputLineIter);
				documentId = (awaitingInputLine == null) ? null : awaitingInputLine.split("\\t")[1];
			}
			System.out.println("============= DOC ID: " + documentId);

			if (awaitingInputLine == null) {
				// then there are no more lines to process
				return null;
			}

			// TODO: process first line for possible entity annotation
			StringBuilder docText = new StringBuilder();
			Packet packet = processToken(docText, awaitingInputLine, awaitingOutputLine);
			lastTag = addOrUpdateEntityAnnot(entityAnnots, documentId, lastTag, packet.getInputToken(),
					packet.getSpanStart(), packet.getOutputTag());

			// the input line iterator is the one that has the document ID info attached to
			// the first token of each sentence
			while (bertInputLineIter.hasNext()) {
				String inputLine = fastForward(bertInputLineIter);
				String outputLine = fastForward(bertOutputLineIter);

				packet = processToken(docText, inputLine, outputLine);

				if (packet == null) {
					// this is a hack - final sentence is missing in the test_labels file for some
					// reason
					// fast forward and then finish
					while (bertInputLineIter.hasNext()) {
						System.err.println("Skipping... " + bertInputLineIter.next().getText());
					}
					break;

				}
				if (packet.getDocumentId().equals("-") || packet.getDocumentId().equals(documentId)) {
					lastTag = addOrUpdateEntityAnnot(entityAnnots, documentId, lastTag, packet.getInputToken(),
							packet.getSpanStart(), packet.getOutputTag());

				} else {
					// the document ID changed, so we are starting a new document
					td = buildDocument(entityAnnots, docText, documentId);
					documentId = packet.getDocumentId();
					awaitingInputLine = inputLine;
					awaitingOutputLine = outputLine;
					return td;
				}

			}

			td = buildDocument(entityAnnots, docText, documentId);
			awaitingInputLine = null;
			return td;

		}

		private TextDocument buildDocument(List<TextAnnotation> entityAnnots, StringBuilder docText,
				String documentId) {
			System.out.println("Building document: " + documentId);
			TextDocument td = new TextDocument(documentId, "doctype", docText.toString());
			td.setAnnotations(entityAnnots);
			return td;
		}

		private Packet processToken(StringBuilder docText, String inputLine, String outputLine) {
//			System.out.println("Input line: " + inputLine);
//			System.out.println("Outpt line: " + outputLine);

			String[] inputCols;
			String[] outputCols;
			String inputToken;
			String outputToken;
			String outputTag;
			int spanStart;
			inputCols = inputLine.split("\\t");
			outputCols = outputLine.split("\\s");

			// check that the tokens are the same
			inputToken = inputCols[0].trim();
			String docId = inputCols[1];
			spanStart = Integer.parseInt(inputCols[2]);
			outputToken = outputCols[0].trim();
			try {
				outputTag = outputCols[2];
			} catch (ArrayIndexOutOfBoundsException e) {
				System.err.println("AIOOB. inputCols: " + Arrays.toString(inputCols) + " -- outputCols: "
						+ Arrays.toString(outputCols));
				return null;
			}
			if (!inputToken.equals(outputToken)) {
				// attempt to remove non-standard whitespace characters
				inputToken = removeNonStandardWhitespace(inputToken);
				outputToken = removeNonStandardWhitespace(outputToken);

				if (!inputToken.equals(outputToken)) {

					String inputTokenCodePoints = Arrays.toString(inputToken.codePoints().toArray());
					String outputTokenCodePoints = Arrays.toString(outputToken.codePoints().toArray());

					throw new IllegalStateException(String.format(
							"Input/output bert tokens don't match. '%s' != '%s'. Input token size: %d -- code points: %s; Output token size: %d -- code points: %s",
							inputToken, outputToken, inputToken.length(), inputTokenCodePoints, outputToken.length(),
							outputTokenCodePoints));
				}
			}
			populateDocumentText(docText, inputToken, spanStart);

			Packet packet = new Packet(docId, inputToken, spanStart, outputToken, outputTag);
			return packet;
		}

		/**
		 * removed non-standard whitespace from the specified input token
		 * 
		 * @param inputToken
		 * @return
		 */
		private String removeNonStandardWhitespace(String inputToken) {
			char hairSpace = 0x200A; // hair space
			char noBreakSpace = 0x00A0; // no-break space
			char thinSpace = 0x2009; // thin space
			Set<Character> nonStandardWhiteSpace = new HashSet<Character>(Arrays.asList(Character.valueOf(hairSpace),
					Character.valueOf(noBreakSpace), Character.valueOf(thinSpace)));
			List<Character> updatedTokenChars = new ArrayList<Character>();
			for (char ic : inputToken.toCharArray()) {
				if (!nonStandardWhiteSpace.contains(Character.valueOf(ic))) {
					updatedTokenChars.add(ic);
				}
			}
			char[] updatedToken = new char[updatedTokenChars.size()];
			for (int i = 0; i < updatedTokenChars.size(); i++) {
				updatedToken[i] = updatedTokenChars.get(i).charValue();
			}
			inputToken = new String(updatedToken);
			return inputToken;
		}

		@Data
		private static class Packet {
			private final String documentId;
			private final String inputToken;
			private final int spanStart;
			private final String outputToken;
			private final String outputTag;
		}

		private void populateDocumentText(StringBuilder docText, String inputToken, int spanStart) {
			while (docText.length() < spanStart) {
				docText.append(" ");
			}
			docText.append(inputToken);
		}

		private String addOrUpdateEntityAnnot(List<TextAnnotation> entityAnnots, String documentId, String lastTag,
				String inputToken, int spanStart, String outputTag) {
			String tag = fixTag(outputTag, lastTag);
			lastTag = tag;

			if (tag.startsWith("B-")) {
				// then this token is the beginning of an entity annotation (or might be an
				// entire entity annotation)
				String label = tag.split("-")[1];
				TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults(documentId);
				TextAnnotation annot = factory.createAnnotation(spanStart, spanStart + inputToken.length(), inputToken,
						label);
				entityAnnots.add(annot);
			} else if (tag.startsWith("I-")) {
				// then this token is likely a continuation of the previous entity annotation
				String label = tag.split("-")[1];
				TextAnnotation annot = entityAnnots.get(entityAnnots.size() - 1);

				// the label should match the annotation label
				if (!label.equals(annot.getClassMention().getMentionName())) {
					throw new IllegalStateException(
							String.format("label mismatch: %s != %s", label, annot.getClassMention().getMentionName()));
				}
				String oldCoveredText = annot.getCoveredText();
				String newCoveredText = addSpaces(oldCoveredText, annot.getAnnotationSpanEnd(), spanStart) + inputToken;
				annot.setCoveredText(newCoveredText);

				int spanEnd = spanStart + inputToken.length();
				annot.setAnnotationSpanEnd(spanEnd);

			} else if (tag.equals("O")) {
				// then this is not part of an entity annotation so do nothing
			}
			return lastTag;
		}

		/**
		 * adds any intervening space(s) between tokens so that the covered text is
		 * correct
		 * 
		 * @param oldCoveredText
		 * @param annotationSpanEnd
		 * @param spanStart
		 * @return
		 */
		private String addSpaces(String oldCoveredText, int annotationSpanEnd, int spanStart) {
			StringBuilder sb = new StringBuilder(oldCoveredText);
			for (int i = annotationSpanEnd; i < spanStart; i++) {
				sb.append(" ");
			}
			return sb.toString();
		}

		/**
		 * Ensures a valid IOB sequence
		 * 
		 * @param string
		 * @param lastLabel
		 * @return
		 */
		protected static String fixTag(String tag, String lastTag) {

			Set<String> outside = new HashSet<String>(Arrays.asList("O"));
			Set<String> inside = new HashSet<String>(Arrays.asList("I", "E"));
			Set<String> begin = new HashSet<String>(Arrays.asList("B", "S"));

			if (outside.contains(tag)) {
				return "O";
			}

			String bioes = tag.split("-")[0];
			String label = tag.split("-")[1];
			String lastLabel = lastTag.equals("O") ? null : lastTag.split("-")[1];

			if ((begin.contains(bioes) && !label.equals(lastLabel)) || outside.contains(lastTag)
					|| !label.equals(lastLabel)) {
				return "B-" + label;
			}

			return "I-" + label;
		}

		/**
		 * advance to the first non-blank line
		 * 
		 * @param bertInputLineIter2
		 */
		private String fastForward(StreamLineIterator lineIter) {
			String line = null;
			while (lineIter.hasNext() && (line == null || line.isEmpty())) {
				line = lineIter.next().getText();
			}
			return line;
		}

		@Override
		public boolean hasNext() {
			return nextDocument != null;
		}

		@Override
		public TextDocument next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			TextDocument toReturn = nextDocument;
			nextDocument = getNextDocument();
			return toReturn;
		}

	}

}
