package edu.cuanschutz.ccp.tm_provider.corpora;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import edu.ucdenver.ccp.common.file.FileWriterUtil;

/**
 * builds scripts to download the Medline baseline and update files
 *
 */
public class MedlineDownloadScriptBuilder {

	public enum PubmedFileSet {
		BASELINE, UPDATEFILES
	}

	public static void createDownloadScript(File scriptFile, PubmedFileSet fileSet, int minFileIndex, int maxFileIndex,
			int twoDigitYear) throws FileNotFoundException, IOException {
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(scriptFile)) {
			for (int i = minFileIndex; i <= maxFileIndex; i++) {
				String index = getIndexStr(i);
				String fileName = String.format("pubmed%dn%s.xml.gz", twoDigitYear, index);
				String command = createCommandStr(fileSet, twoDigitYear, fileName);
				writer.write(command);
				fileName = fileName + ".md5";
				command = createCommandStr(fileSet, twoDigitYear, fileName);
				writer.write(command);
			}
		}
	}

	private static String createCommandStr(PubmedFileSet fileSet, int twoDigitYear, String fileName) {
		String commandStr = "curl ftp://ftp.ncbi.nlm.nih.gov/pubmed/%s/%s | "
				+ "gsutil cp - gs://translator-text-workflow-dev_work/text-corpora/medline-xml/%s_20%d/%s\n";
		String command = String.format(commandStr, fileSet.name().toLowerCase(), fileName, fileSet.name().toLowerCase(),
				twoDigitYear, fileName);
		return command;
	}

	protected static String getIndexStr(int i) {
		StringBuilder prefix = new StringBuilder();
		if (i / 1000 == 0) {
			prefix.append("0");
		}
		if (i / 100 == 0) {
			prefix.append("0");
		}
		if (i / 10 == 0) {
			prefix.append("0");
		}
		return String.format("%s%d", prefix, i);

	}

	public static void main(String[] args) {

		int twoDigitYear = 22;
		try {

			{
				PubmedFileSet fileSet = PubmedFileSet.BASELINE;
				int minFileIndex = 1;
				int maxFileIndex = 1114;
				File outputFile = new File(
						String.format("/Users/bill/projects/ncats-translator/corpora/download_%s_20%d.sh",
								fileSet.name().toLowerCase(), twoDigitYear));
				createDownloadScript(outputFile, fileSet, minFileIndex, maxFileIndex, twoDigitYear);
			}

			{
				PubmedFileSet fileSet = PubmedFileSet.UPDATEFILES;
				int minFileIndex = 1115;
				int maxFileIndex = 1343;
				File outputFile = new File(
						String.format("/Users/bill/projects/ncats-translator/corpora/download_%s_20%d.sh",
								fileSet.name().toLowerCase(), twoDigitYear));
				createDownloadScript(outputFile, fileSet, minFileIndex, maxFileIndex, twoDigitYear);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
