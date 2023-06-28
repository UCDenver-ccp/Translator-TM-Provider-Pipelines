package edu.cuanschutz.ccp.tm_provider.oger.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.reader.Line;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import lombok.Data;

/**
 * Reads the contents of an embedding file where the first column is the word/id
 * and remaining columns are elements of the embedding vector.
 *
 */
/**
 * @author bill
 *
 */
public class EmbeddingUtil {

	public static Map<String, Embedding> loadEmbeddingFile(File embeddingFile, String delimiterRegex)
			throws IOException {

		Map<String, Embedding> map = new HashMap<String, Embedding>();

		int embeddingDimension = -1;
		for (StreamLineIterator lineIter = new StreamLineIterator(
				new GZIPInputStream(new FileInputStream(embeddingFile)), CharacterEncoding.UTF_8, null); lineIter
						.hasNext();) {
			Line line = lineIter.next();
			if (line.getLineNumber() % 1000 == 0) {
				System.out.println("Load progress: " + line.getLineNumber());
			}
			if (line.getLineNumber() == 0) {
				// this is the header. It contains two numbers: (1) the number of rows in the
				// file; and (2) the dimension of the embedding (# of columns)
				String[] cols = line.getText().split(delimiterRegex);
				if (cols.length == 2) {
					embeddingDimension = Integer.parseInt(cols[1]);
				} else {
					throw new IllegalArgumentException("Unexpected number of columns in header: " + cols.length);
				}
			} else {
				String[] cols = line.getText().split(delimiterRegex);
				if (cols.length == embeddingDimension + 1) {

					String id = cols[0];
					double[] embeddingVector = new double[embeddingDimension];
					for (int i = 1; i < embeddingDimension + 1; i++) {
						embeddingVector[i - 1] = Double.parseDouble(cols[i]);
					}
					Embedding e = new Embedding(id, embeddingVector);
					map.put(id, e);

				} else {
					throw new IllegalArgumentException(
							String.format("Unexpected number of columns (%d != %d (expected)) on line %d: ",
									cols.length, embeddingDimension, line.getLineNumber()));
				}
			}
		}

		return map;
	}

	@Data
	public static class Embedding {
		private final String id;
		private final double[] embeddingVector;
	}

	/**
	 * Computes the average of the input embedding vectors
	 * 
	 * @param averageEmbedding
	 * @param embedding
	 * @return
	 */
	public static double[] average(double[] embedding1, double[] embedding2) {

		double[] average = new double[embedding1.length];

		for (int i = 0; i < embedding1.length; i++) {
			average[i] = (embedding1[i] + embedding2[i]) / 2;
		}

		return average;
	}

	public static void serializeEmbedding(File embeddingFile, double[] embedding) throws IOException {
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(embeddingFile)) {
			writer.write(embedding.length + "\n");
			for (double val : embedding) {
				writer.write(val + "\n");
			}
		}
	}

	public static double[] deserializeEmbedding(File embeddingFile) throws IOException {
		double[] embedding = null;
		for (StreamLineIterator lineIter = new StreamLineIterator(embeddingFile, CharacterEncoding.UTF_8); lineIter
				.hasNext();) {
			Line line = lineIter.next();

			if (line.getLineNumber() == 0) {
				// the header contains the dimension of the serialized embedding vector
				int embeddingDimension = Integer.parseInt(line.getText());
				embedding = new double[embeddingDimension];
			} else {
				embedding[(int) line.getLineNumber() - 1] = Double.parseDouble(line.getText());
			}
		}
		return embedding;

	}

	/**
	 * From:
	 * https://stackoverflow.com/questions/520241/how-do-i-calculate-the-cosine-similarity-of-two-vectors
	 * 
	 * @param vectorA
	 * @param vectorB
	 * @return
	 */
	public static double cosineSimilarity(double[] vectorA, double[] vectorB) {
		double dotProduct = 0.0;
		double normA = 0.0;
		double normB = 0.0;
		for (int i = 0; i < vectorA.length; i++) {
			dotProduct += vectorA[i] * vectorB[i];
			normA += Math.pow(vectorA[i], 2);
			normB += Math.pow(vectorB[i], 2);
		}
		return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
	}

}
