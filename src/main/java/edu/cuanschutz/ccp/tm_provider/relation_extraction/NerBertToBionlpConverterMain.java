package edu.cuanschutz.ccp.tm_provider.relation_extraction;

import java.io.File;
import java.io.IOException;

public class NerBertToBionlpConverterMain {

	public static void main(String[] args) {
//		File dataDir = new File("/Users/bill/projects/ncats-translator/concept-recognition/bert-ner/craft-ner-test");
		File dataDir = new File("/Users/bill/projects/ncats-translator/concept-recognition/bert-ner/craft-ner-dev");

		File bertInputFile = new File(dataDir, "craft-ner.test.iob.bert");
		File bertOutputFile = new File(dataDir, "test_labels.txt");
		File outputDir = new File(dataDir, "bionlp");

		try {
			NerBertToBionlpConverter.convert(bertInputFile, bertOutputFile, outputDir);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

}
