package edu.cuanschutz.ccp.tm_provider.corpora.semmed;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.reader.Line;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;

public class SemmedDbFebRelay2023GoogleSheetsCreatorMain {
	public static void main(String[] args) {

		// biostacks
//		File credentialsFile = new File(
//				"/Users/bill/Documents/credentials/client_secret_575571346320-a5cgdgfukscij6av346qtg0747pcce1a.apps.googleusercontent.com-2.json");

		// gmail
		File credentialsFile = new File(
				"/Users/bill/Downloads/client_secret_1045818103889-od3pfutus7np5sdmsh2l9l4bm9ddavbi.apps.googleusercontent.com_main.json");

		// stores authentication tokens (I think)

		/*
		 * Note: The tokens expire periodically (after 1 week I think). In order to
		 * refresh - delete the file in the dataStoreDirectory and then the next time
		 * this is run you will be prompted to open a web page in a browser to
		 * authenticate.
		 * 
		 * I also tried removing and re-adding the user from the gcp console API OAuth
		 * consent screen - that alone did not help, nor did refreshing the Sheet API
		 * credentials on the API credentials screen.
		 */

		File tokensDirectory = new File("/Users/bill/Documents/credentials/tokens");
		if (!tokensDirectory.exists()) {
			tokensDirectory.mkdirs();
		}

		String folderId = "1hVg46HqaRK5Y7i8FD9wKEpp4paMyVWOg";

		int batchSize = 50;
		int batchNum = 103;
		int batchOverlap = 5;

		File predicationsFile = new File(
				"/Users/bill/projects/ncats-translator/relay_feb_2023/from_db/semmed_treats_sample_5000.csv.gz");
		createSheets(credentialsFile, tokensDirectory, batchNum, batchSize, batchOverlap, predicationsFile, folderId);

	}

	/**
	 * @param credentialsFile
	 * @param tokenDirectory
	 * @param batchSize
	 * @param samplePredicationsFile
	 * @param folderId               - where the sheets will be moved once they are
	 *                               created
	 */
	private static void createSheets(File credentialsFile, File tokenDirectory, int batchNum, int batchSize,
			int batchOverlap, File samplePredicationsFile, String folderId) {
		try {
			SemmedDbFebRelay2023GoogleSheetsCreator creator = new SemmedDbFebRelay2023GoogleSheetsCreator(
					tokenDirectory);

			Set<String> alreadyUsed = new HashSet<String>(Arrays.asList("155918266", "159027334", "140970079",
					"176994455", "187719139", "179424312", "164075193", "189364869", "169303422", "86835082",
					"93331046", "89734003", "164078026", "91111879", "167415877", "165824190", "199391447", "196679754",
					"168588514", "198204825", "159009177", "108257517", "129178379", "134607148", "109833824",
					"194789610", "173600534", "94272582", "114010286", "186436816", "179011838", "173220354",
					"73216004", "109998619", "155221388", "163527487", "134498275", "156756201", "158862935",
					"84122877", "129928163", "102669706", "125607180", "95609264", "126619965", "34751483", "173215534",
					"82308999", "182833105", "81937285", "155918266", "159027334", "140970079", "176994455",
					"187719139", "179424312", "164075193", "189364869", "169303422", "86835082", "93331046", "89734003",
					"164078026", "91111879", "167415877", "165824190", "199391447", "196679754", "168588514",
					"198204825", "159009177", "108257517", "129178379", "134607148", "109833824", "129763948",
					"197750928", "170029906", "184338351", "101165323", "142794194", "151634941", "169203486",
					"157226341", "111562965", "140224425", "88347689", "183113976", "80443875", "184521856", "93456039",
					"186265658", "166286974", "193605811", "110770849", "85030831", "83727327", "95305821", "126684620",
					"17271686"));

			List<List<String>> predicationBatches = createPredicationBatches(samplePredicationsFile, batchNum,
					batchSize, batchOverlap, alreadyUsed);

			System.out.println("Batch count: " + predicationBatches.size());

			int index = 3;
			List<String> sheetIds = new ArrayList<String>();
			for (List<String> batch : predicationBatches) {
				String sheetTitle = String.format("batch_%d", index++);
				String sheetId = creator.createNewSpreadsheet(credentialsFile, batch, sheetTitle, folderId);
				sheetIds.add(sheetId);
				Thread.sleep(5000);
			}

			creator.createIndexSpreadsheet(credentialsFile, sheetIds);

		} catch (IOException | GeneralSecurityException | InterruptedException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	private static List<List<String>> createPredicationBatches(File samplePredicationsFile, int batchNum, int batchSize,
			int batchOverlap, Set<String> alreadyUsed) throws FileNotFoundException, IOException {
		List<List<String>> batches = new ArrayList<List<String>>();

		List<String> batch1 = new ArrayList<String>();
		List<String> batch2 = new ArrayList<String>();

		boolean overlap = true;
		int overlapCount = 0;
		for (StreamLineIterator lineIter = new StreamLineIterator(
				new GZIPInputStream(new FileInputStream(samplePredicationsFile)), CharacterEncoding.UTF_8,
				null); lineIter.hasNext();) {
			Line line = lineIter.next();

			if (line.getLineNumber() == 0) {
				// skip header
				continue;
			}

			System.out.println(line.getText());

			String[] cols = line.getText().split(",");
			String predicationId = cols[0];

			if (alreadyUsed.contains(predicationId)) {
				continue;
			}

			if (overlap) {
				// add the predicationId to both batches
				System.out.println("overlap -- adding to both");
				batch1.add(predicationId);
				batch2.add(predicationId);
				overlapCount++;
				if (overlapCount == batchOverlap) {
					overlap = false;
				}
			} else {
				System.out.println("not overlap");
				// add every other predication Id to batch1, the other to batch2
				if (line.getLineNumber() % 2 == 0) {
					batch1.add(predicationId);
				} else {
					batch2.add(predicationId);
				}
			}

			if (batch1.size() == batchSize && batch2.size() == batchSize) {
				// these batches are complete so we will save them and start new batches
				batches.add(batch1);
				batches.add(batch2);
				overlap = true;
				overlapCount = 0;
				batch1 = new ArrayList<String>();
				batch2 = new ArrayList<String>();
			}

			if (batches.size() >= batchNum) {
				// stop when we reach the number of batches requested
				System.out.println("breaking");
				break;

			}
		}

		return batches;
	}
}
