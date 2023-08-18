package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.reader.Line;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;

/**
 * This was used to create the file that was manually curated to indicate PR
 * synonyms to exclude.
 *
 */
public class PrEnglishWordFilter {

	public static void main(String[] args) {
		File file = new File(
				"/Users/bill/projects/ncats-translator/ontology-resources/ontologies/20230716/english-pr.out");
		File outputFile = new File(
				"/Users/bill/projects/ncats-translator/ontology-resources/ontologies/20230716/english-pr.filtered.out");

		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(outputFile)) {
			for (StreamLineIterator lineIter = new StreamLineIterator(file, CharacterEncoding.UTF_8); lineIter
					.hasNext();) {
				Line line = lineIter.next();

				String[] cols = line.getText().split("\\t");
				String iri = cols[0];
				String label = cols[1];

				if (label.length() > 2) {
					writer.write(String.format("\t%s\t%s\t%s\n", label.toLowerCase(), label,
							iri.substring(iri.lastIndexOf("/") + 1)));
				}

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
