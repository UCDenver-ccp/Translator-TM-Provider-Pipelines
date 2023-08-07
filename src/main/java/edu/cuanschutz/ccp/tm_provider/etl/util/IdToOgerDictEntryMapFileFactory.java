package edu.cuanschutz.ccp.tm_provider.etl.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.reader.Line;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import lombok.Data;

/**
 * Creates a mapping from concept ID to OGER dictionary entries
 *
 */
@Data
public class IdToOgerDictEntryMapFileFactory {

	public static void createMappingFile(File outputFile, File... inputFiles) throws IOException {

		Map<String, Set<String>> idToLabelMap = new HashMap<String, Set<String>>();

		for (File file : inputFiles) {
			for (StreamLineIterator lineIter = new StreamLineIterator(file, CharacterEncoding.UTF_8); lineIter
					.hasNext();) {
				Line line = lineIter.next();
				if (line.getLineNumber() % 10000 == 0) {
					System.out.println(String.format("%s progress: %d...", file.getName(), line.getLineNumber()));
				}
				String[] cols = line.getText().split("\\t");
				String id = cols[2];
				String label = cols[3];

				CollectionsUtil.addToOne2ManyUniqueMap(id, label, idToLabelMap);
			}
		}

		try (BufferedWriter writer = FileWriterUtil
				.initBufferedWriter(new GZIPOutputStream(new FileOutputStream(outputFile)), CharacterEncoding.UTF_8)) {
			for (Entry<String, Set<String>> entry : idToLabelMap.entrySet()) {
				writeMapping(writer, entry.getKey(), entry.getValue());
			}
		}
	}

	private static void writeMapping(BufferedWriter writer, String id, Set<String> labels) throws IOException {
		String labelStr = CollectionsUtil.createDelimitedString(labels, "|");
		writer.write(String.format("%s\t%s\n", id, labelStr));
	}

	public static void main(String[] args) {
		File baseDir = new File("/Users/bill/projects/ncats-translator/prototype/oger-docker.git/dict/20230716");
		File outputFile = new File(baseDir, "idToDictEntryMap.tsv.gz");
		File csFile = new File(baseDir, "case_sensitive.dict.tsv");
		File ciminFile = new File(baseDir, "case_insensitive.min_norm.dict.tsv");
		File cimaxFile = new File(baseDir, "case_insensitive.max_norm.dict.tsv");

		try {
			createMappingFile(outputFile, csFile, ciminFile, cimaxFile);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
