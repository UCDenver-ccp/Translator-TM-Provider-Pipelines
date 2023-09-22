package edu.cuanschutz.ccp.tm_provider.relation_extraction;

import static edu.cuanschutz.ccp.tm_provider.etl.fn.ElasticsearchDocumentCreatorFn.computeSentenceIdentifier;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileReaderUtil;
import edu.ucdenver.ccp.common.file.FileUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil;

/**
 * Reads through sentence files in a directory and adds sentence id's (from the
 * ElasticsearchBratExporter.computeHash() method) to a file.
 *
 */
public class SentenceIdFileGenerator {

	private static final CharacterEncoding UTF8 = CharacterEncoding.UTF_8;

	public static void catalogSentences(File baseDir, File outputFile) throws IOException {
		Set<String> ids = new HashSet<String>();
		for (Iterator<File> fileIterator = FileUtil.getFileIterator(baseDir, true, ".txt"); fileIterator.hasNext();) {
			File txtFile = fileIterator.next();

			List<String> sentences = FileReaderUtil.loadLinesFromFile(txtFile, UTF8);
			for (String sentence : sentences) {
				if (!sentence.equals("DONE")) {
					String hash = computeSentenceIdentifier(sentence);
					ids.add(hash);
				}
			}
		}
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(outputFile)) {
			for (String id : ids) {
				writer.write(id + "\n");
			}
		}
		System.out.println(String.format("Observed %d sentences.", ids.size()));
	}

	public static void main(String[] args) {
		File baseDir = new File(args[0]);
		File outputFile = new File(baseDir, "sentence.ids");

		try {
			catalogSentences(baseDir, outputFile);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}

	}
}
