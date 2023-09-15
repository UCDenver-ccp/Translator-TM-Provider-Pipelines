package edu.cuanschutz.ccp.tm_provider.kg;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.reader.Line;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;

public class KgxUtil {

	public static final String KGX_NODE_HEADER = CollectionsUtil
			.createDelimitedString(Arrays.asList("id", "name", "category"), "\t");

	public static final String KGX_EDGE_HEADER = CollectionsUtil.createDelimitedString(
			Arrays.asList("subject", "edge_label", "object", "relation", "id", "association_type"), "\t");

	public static final String KGX_EDGE_WITH_EVIDENCE_HEADER = CollectionsUtil
			.createDelimitedString(Arrays.asList("subject", "edge_label", "object", "relation", "id",
					"association_type", "evidence_count", "has_evidence"), "\t");

	public static final String KGX_NODE_WITH_EVIDENCE_HEADER = CollectionsUtil
			.createDelimitedString(Arrays.asList("id", "name", "category", "publications", "score", "sentence",
					"subject_spans", "relation_spans", "object_spans", "provided_by"), "\t");

	public static void validateFile(File tsvFile, String header) throws FileNotFoundException, IOException {
		System.out.println("Validating file: " + tsvFile.getAbsolutePath());
		int expectedColumnCount = header.split("\\t").length;
		for (StreamLineIterator lineIter = new StreamLineIterator(new GZIPInputStream(new FileInputStream(tsvFile)),
				CharacterEncoding.UTF_8, null); lineIter.hasNext();) {
			Line line = lineIter.next();
			String[] cols = line.getText().split("\\t", -1);
			int colCount = cols.length;
			if (colCount != expectedColumnCount) {
				for (int i = 0; i < cols.length; i++) {
					System.out.println("COL " + i + ": " + cols[i]);
				}
				throw new IllegalStateException(
						String.format("Unexpected number of columns (Line %d). Observed %d but expected %d",
								line.getLineNumber(), colCount, expectedColumnCount));
			}
		}

	}

}
