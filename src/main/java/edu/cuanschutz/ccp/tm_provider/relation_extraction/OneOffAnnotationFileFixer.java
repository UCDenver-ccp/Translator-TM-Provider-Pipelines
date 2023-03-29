package edu.cuanschutz.ccp.tm_provider.relation_extraction;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileUtil;
import edu.ucdenver.ccp.common.string.StringUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentReader;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;

/**
 * 
 * Created to fix annotation files by removing references to PMIDs in the
 * document text b/c these references highlight which sentences are being used
 * to compute IAA.
 * 
 */
public class OneOffAnnotationFileFixer {

	private static final CharacterEncoding ENCODING = CharacterEncoding.UTF_8;

	public static void removePmidReferences(File inputDirectory, File outputDirectory) throws IOException {
		for (Iterator<File> fileIterator = FileUtil.getFileIterator(inputDirectory, false, ".ann"); fileIterator
				.hasNext();) {
			File annFile = fileIterator.next();
			File txtFile = new File(annFile.getParentFile(),
					StringUtil.removePrefix(annFile.getName(), ".ann") + ".txt");

			BioNLPDocumentReader docReader = new BioNLPDocumentReader();
			TextDocument td = docReader.readDocument("sourceid", "sourcedb", annFile, txtFile, ENCODING);

			String[] sentences = td.getText().split("\\n");
			int[] startIndexes = new int[sentences.length];
			int index = 0;
			for (int i = 0; i < startIndexes.length; i++) {
				startIndexes[i] = index;
				index = sentences[i].length() + 1;
			}

			for (int i = sentences.length; i >= 0; i--) {
				String sentence = sentences[i];
				if (StringUtil.endsWithRegex(sentence, " -- PMID:\\d+")) {
					String updatedSentence = StringUtil.removeSuffixRegex(sentence, " -- PMID:\\d+");

					int offset = sentence.length() - updatedSentence.length();
					sentences[i] = updatedSentence;

					// for any annotations that start after this sentence, remove "offset" from
					// their spans
					int sentenceStart = startIndexes[i];
					for (TextAnnotation annot : td.getAnnotations()) {
						if (annot.getAggregateSpan().getSpanStart() > sentenceStart + sentence.length()) {
							List<Span> updatedSpans = new ArrayList<Span>();
							for (Span span : annot.getSpans()) {
								updatedSpans.add(new Span(span.getSpanStart() - offset, span.getSpanEnd() - offset));
							}
							annot.setSpans(updatedSpans);
						}
					}
				}
			}
		}
	}
}
