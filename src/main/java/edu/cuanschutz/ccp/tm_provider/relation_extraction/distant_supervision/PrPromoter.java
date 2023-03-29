package edu.cuanschutz.ccp.tm_provider.relation_extraction.distant_supervision;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;

/**
 * Promote Protein Ontology concepts in the concept pairs files to their
 * species-agnostic version so that when we search in Elastic we find them.
 *
 */
public class PrPromoter {

	public static void createFileWithPromotedPrConcepts(File inputFile, File outputFile, File prPromotionMapFile)
			throws FileNotFoundException, IOException {
		Map<String, String> prPromotionMap = loadPrPromotionMap(prPromotionMapFile);

		try (BufferedWriter writer = FileWriterUtil
				.initBufferedWriter(new GZIPOutputStream(new FileOutputStream(outputFile)), CharacterEncoding.UTF_8)) {
			for (StreamLineIterator lineIter = new StreamLineIterator(
					new GZIPInputStream(new FileInputStream(inputFile)), CharacterEncoding.UTF_8, null); lineIter
							.hasNext();) {
				String[] cols = lineIter.next().getText().split("\\t");
				String prConcept = cols[0];
				if (prPromotionMap.containsKey(prConcept)) {
					prConcept = prPromotionMap.get(prConcept);
//					System.out.println("promoted!");
				} else {
					System.out.println("NOT PROMOTED! " + prConcept);
				}
				writer.write(prConcept + "\t" + cols[1] + "\t" + cols[2] + "\n");
				
			}
		}
	}

	private static Map<String, String> loadPrPromotionMap(File prPromotionMapFile)
			throws FileNotFoundException, IOException {
		Map<String, String> map = new HashMap<String, String>();
		for (StreamLineIterator lineIter = new StreamLineIterator(
				new GZIPInputStream(new FileInputStream(prPromotionMapFile)), CharacterEncoding.UTF_8, null); lineIter
						.hasNext();) {
			String[] cols = lineIter.next().getText().split("\\t");

			map.put(cols[0].replace(":", "_"), cols[1].replace(":", "_"));

		}
		return map;
	}

	public static void main(String[] args) {

		File prPromotionMapFile = new File(
				"/Users/bill/projects/ncats-translator/relations/distant-supervision/concept-pair-files/pr-promotion-map.tsv.gz");

//		File inputFile = new File(
//				"/Users/bill/projects/ncats-translator/relations/distant-supervision/concept-pair-files/protein-go_cc.tsv.gz");
//		File outputFile = new File(
//				"/Users/bill/projects/ncats-translator/relations/distant-supervision/concept-pair-files/protein-go_cc.pr_promoted.tsv.gz");

		File inputFile = new File(
				"/Users/bill/projects/ncats-translator/relations/distant-supervision/concept-pair-files/protein-cell.cl.tsv.gz");
		File outputFile = new File(
				"/Users/bill/projects/ncats-translator/relations/distant-supervision/concept-pair-files/protein-cell.cl.pr_promoted.tsv.gz");

//		File inputFile = new File(
//				"/Users/bill/projects/ncats-translator/relations/distant-supervision/concept-pair-files/protein-anatomy.tsv.gz");
//		File outputFile = new File(
//				"/Users/bill/projects/ncats-translator/relations/distant-supervision/concept-pair-files/protein-anatomy.pr_promoted.tsv.gz");
		try {
			createFileWithPromotedPrConcepts(inputFile, outputFile, prPromotionMapFile);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}

	}

}
