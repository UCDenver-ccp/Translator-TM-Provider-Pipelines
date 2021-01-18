package edu.cuanschutz.ccp.tm_provider.relation_extraction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;

public class BertPredictOutputCompiler {

	private static final String UP = "UP";
	private static final String DOWN = "DOWN";

	public static void main(String[] args) {
		File dir = new File(
				"/Users/bill/projects/ncats-translator/biolink-text-mining/relation-extraction/chebi-pr/chebi-pr-sentences");
//		File inputFile = new File(dir, "chebi-pr-input.tsv.gz");
//		File resultsFile = new File(dir, "test_results.tsv.gz");
//		File outputFile = new File(dir, "tagged_results.tsv.gz");
		
		File inputFile = new File(dir, "chebi-pr-sentences.filtered.tsv.gz");
		File resultsFile = new File(dir, "chebi-pr-sentences.filtered.test_results.tsv.gz");
		File outputFile = new File(dir, "tagged-chebi-pr-sentences.filtered.test_results.tsv.gz");

		// bert script seems to assume first line in the input is a header, so it is
		// ignored, so the first line in the results file corresponds to the second line
		// in the input file

		try (BufferedWriter writer = FileWriterUtil
				.initBufferedWriter(new GZIPOutputStream(new FileOutputStream(outputFile)), CharacterEncoding.UTF_8)) {
			StreamLineIterator inputLineIter = new StreamLineIterator(
					new GZIPInputStream(new FileInputStream(inputFile)), CharacterEncoding.UTF_8, null);

			if (inputLineIter.hasNext()) {
				// burn first line as it is assumed to be a header by the bert script
				inputLineIter.next();
			}
			int count = 0;

			for (StreamLineIterator resultLineIter = new StreamLineIterator(
					new GZIPInputStream(new FileInputStream(resultsFile)), CharacterEncoding.UTF_8,
					null); resultLineIter.hasNext();) {
				if (count++ % 10000 == 0) {
					System.out.println("progress... " + (count - 1));
				}
				String resultLine = resultLineIter.next().getText();
				String inputLine = (inputLineIter.hasNext()) ? inputLineIter.next().getText() : null;
				if (inputLine == null) {
					throw new IllegalArgumentException("expected same number of lines in input and results files");
				}
				String[] cols = resultLine.split("\\t");
				double upRegulationScore = Double.parseDouble(cols[0]);
				double downRegulationScore = Double.parseDouble(cols[1]);

				if (upRegulationScore > 0.95) {
					writer.write(UP + "\t" + upRegulationScore + "\t" + inputLine + "\n");
				} else if (downRegulationScore > 0.95) {
					writer.write(DOWN + "\t" + downRegulationScore + "\t" + inputLine + "\n");
				}
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
