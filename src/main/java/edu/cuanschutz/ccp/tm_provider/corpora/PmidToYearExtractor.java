package edu.cuanschutz.ccp.tm_provider.corpora;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBElement;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Unmarshaller;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.medline.ArticleDate;
import org.medline.MedlineCitation;
import org.medline.MedlineDate;
import org.medline.PubmedArticle;
import org.medline.PubmedArticleSet;
import org.medline.Year;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil.FileSuffixEnforcement;
import edu.ucdenver.ccp.common.file.FileWriterUtil.WriteMode;

/**
 * Extracts PMIDs and the year published from Medline XML files
 *
 */
public class PmidToYearExtractor {

	protected static void extract(InputStream xmlStream, File outputDirectory, String inputFileName)
			throws JAXBException, XMLStreamException, IOException {

		File outputFile = new File(outputDirectory, inputFileName + ".pubyear.tsv");

		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(outputFile, CharacterEncoding.UTF_8,
				WriteMode.APPEND, FileSuffixEnforcement.OFF)) {

			JAXBContext context = JAXBContext.newInstance(PubmedArticleSet.class);
			Unmarshaller um = context.createUnmarshaller();

			XMLInputFactory xmlFactory = XMLInputFactory.newInstance();
			XMLStreamReader reader = xmlFactory.createXMLStreamReader(xmlStream);

			// interested in "PubmedArticle" elements only. Skip up to first "PubmedArticle"
			while (reader.hasNext() && (!reader.isStartElement() || !reader.getLocalName().equals("PubmedArticle"))) {
				reader.next();
			}

			while (reader.getEventType() == XMLStreamConstants.START_ELEMENT) {
				JAXBElement<PubmedArticle> element = um.unmarshal(reader, PubmedArticle.class);
				PubmedArticle article = element.getValue();

				MedlineCitation medlineCitation = article.getMedlineCitation();

				String pmid = medlineCitation.getPMID().getvalue();

				List<Object> yearOrMonthOrDayOrSeasonOrMedlineDate = medlineCitation.getArticle().getJournal()
						.getJournalIssue().getPubDate().getYearOrMonthOrDayOrSeasonOrMedlineDate();
				List<ArticleDate> articleDate = medlineCitation.getArticle().getArticleDate();

				String year = getYear(yearOrMonthOrDayOrSeasonOrMedlineDate, articleDate);
				if (year != null) {
					writer.write("PMID:" + pmid + "\t" + year + "\n");
				} else {
					throw new IllegalArgumentException("Null year for PMID: " + pmid + " in file " + inputFileName);
				}

				while (reader.hasNext()
						&& (!reader.isStartElement() || !reader.getLocalName().equals("PubmedArticle"))) {
					reader.next();
				}

			}

			reader.close();

		}

	}

	private static String getYear(List<Object> yearOrMonthOrDayOrSeasonOrMedlineDate, List<ArticleDate> articleDate) {
		for (Object obj : yearOrMonthOrDayOrSeasonOrMedlineDate) {
			if (obj instanceof MedlineDate) {
				MedlineDate md = (MedlineDate) obj;
				String dateStr = md.getvalue();
				String year = getYear(dateStr);
				if (year != null) {
					return year;
				}
			} else if (obj instanceof Year) {
				Year y = (Year) obj;
				if (y != null) {
					return y.getvalue();
				}
			}
		}

		for (ArticleDate ad : articleDate) {
			Year y = ad.getYear();
			if (y != null) {
				return y.getvalue();
			}
		}
		return null;
	}

	/**
	 * assumes that a sequence of 4 integers is the year
	 * 
	 * @param dateStr
	 * @return
	 */
	private static String getYear(String dateStr) {
		Pattern p = Pattern.compile("\\b(\\d\\d\\d\\d)\\b");
		Matcher m = p.matcher(dateStr);
		if (m.find()) {
			String year = m.group(1);
			return year;
		}
		return null;
	}

	public static void main(String[] args) {

		File xmlDirectory = new File(args[0]);
		File outputDirectory = new File(args[1]);
		int skip = Integer.parseInt(args[2]);
		int count = 0;
		try {
			for (Iterator<File> fileIterator = FileUtil.getFileIterator(xmlDirectory, false, ".gz",
					".xml"); fileIterator.hasNext();) {
				File file = fileIterator.next();

				if (count++ >= skip) {
					System.out.println("Processing: " + file.getAbsolutePath());
					try {
						InputStream xmlStream = (file.getName().endsWith(".gz"))
								? new GZIPInputStream(new FileInputStream(file))
								: new FileInputStream(file);
						extract(xmlStream, outputDirectory, file.getName());
					} catch (JAXBException | XMLStreamException | IOException e) {
						System.err.println("Error processing file: " + file.getName());
						e.printStackTrace();
					}
				} else {
					System.out.println("Skipping: " + file.getAbsolutePath());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}

	}
}
