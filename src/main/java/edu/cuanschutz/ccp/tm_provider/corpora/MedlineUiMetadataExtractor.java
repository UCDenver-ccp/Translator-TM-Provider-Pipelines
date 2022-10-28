package edu.cuanschutz.ccp.tm_provider.corpora;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.beam.vendor.grpc.v1p26p0.com.jcraft.jzlib.GZIPInputStream;
import org.medline.ArticleDate;
import org.medline.Day;
import org.medline.DeleteCitation;
import org.medline.MedlineCitation;
import org.medline.MedlineDate;
import org.medline.Month;
import org.medline.PMID;
import org.medline.PubmedArticle;
import org.medline.PubmedArticleSet;
import org.medline.Year;

import edu.cuanschutz.ccp.tm_provider.etl.fn.MedlineXmlToTextFn;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil;

/**
 * Initially designed to extract publication metadata for the Translator UI team
 * from Medline XML.
 *
 */
public class MedlineUiMetadataExtractor {

	private static final String HEADER = "DOC_ID\tyear\tmonth\tday\tjournal\tjournal_abbrev\tvolume\tissue\tarticle_title\tarticle_abstract";

	protected static void extract(InputStream xmlStream, File outputDirectory, String inputFileName)
			throws JAXBException, XMLStreamException, IOException {

		File outputFile = new File(outputDirectory, inputFileName + ".ui_metadata.tsv.gz");
		// list pmids that should be deleted
		File deleteFile = new File(outputDirectory, inputFileName + ".ui_metadata.delete.tsv.gz");

		try (BufferedWriter writer = FileWriterUtil
				.initBufferedWriter(new GZIPOutputStream(new FileOutputStream(outputFile)), CharacterEncoding.UTF_8)) {

			writeHeader(writer);

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
				String month = getMonth(yearOrMonthOrDayOrSeasonOrMedlineDate, articleDate);
				String day = getDay(yearOrMonthOrDayOrSeasonOrMedlineDate, articleDate);
				String journalNameAbbreviation = medlineCitation.getArticle().getJournal().getISOAbbreviation();
				String journalName = medlineCitation.getArticle().getJournal().getTitle();
				String journalIssue = medlineCitation.getArticle().getJournal().getJournalIssue().getIssue();
				String journalVolume = medlineCitation.getArticle().getJournal().getJournalIssue().getVolume();
				String articleTitle = MedlineXmlToTextFn.extractTitleText(medlineCitation);
				String abstractText = MedlineXmlToTextFn.getAbstractText(article);

				String metadataLine = String.format("PMID:%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s", pmid,
						replaceWithHyphenIfNull(year), replaceWithHyphenIfNull(month), replaceWithHyphenIfNull(day),
						replaceWithHyphenIfNull(journalName), replaceWithHyphenIfNull(journalNameAbbreviation),
						replaceWithHyphenIfNull(journalVolume), replaceWithHyphenIfNull(journalIssue),
						replaceLineBreaks(replaceWithHyphenIfNull(articleTitle)),
						replaceLineBreaks(replaceWithHyphenIfNull(abstractText)));
				writer.write(metadataLine + "\n");

				while (reader.hasNext()
						&& (!reader.isStartElement() || !reader.getLocalName().equals("PubmedArticle"))) {
					reader.next();
					if (reader.isStartElement() && reader.getLocalName().equals("DeleteCitation")) {
						JAXBElement<DeleteCitation> deleteElement = um.unmarshal(reader, DeleteCitation.class);
						DeleteCitation dc = deleteElement.getValue();
						List<PMID> pmidsToBeDeleted = dc.getPMID();
						try (BufferedWriter deleteWriter = FileWriterUtil.initBufferedWriter(
								new GZIPOutputStream(new FileOutputStream(deleteFile)), CharacterEncoding.UTF_8)) {
							for (PMID pmidToDelete : pmidsToBeDeleted) {
								deleteWriter.write("PMID:" + pmidToDelete.getvalue() + "\n");
							}
						}
					}
				}

			}

			reader.close();

		}

	}

	private static void writeHeader(BufferedWriter writer) throws IOException {
		writer.write(HEADER + "\n");
	}

	private static String replaceWithHyphenIfNull(String s) {
		if (s == null) {
			return "-";
		}
		return s;
	}

	private static String replaceLineBreaks(String s) {
		return s.replaceAll("[\\n\\t]", " ");
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

	private static String getMonth(List<Object> yearOrMonthOrDayOrSeasonOrMedlineDate, List<ArticleDate> articleDate) {
		for (Object obj : yearOrMonthOrDayOrSeasonOrMedlineDate) {
			if (obj instanceof MedlineDate) {
				MedlineDate md = (MedlineDate) obj;
				String dateStr = md.getvalue();
				String month = getMonth(dateStr);
				if (month != null) {
					return month;
				}
			} else if (obj instanceof Month) {
				Month m = (Month) obj;
				if (m != null) {
					return m.getvalue();
				}
			}
		}

		for (ArticleDate ad : articleDate) {
			Month m = ad.getMonth();
			if (m != null) {
				return m.getvalue();
			}
		}
		return null;
	}

	private static String getMonth(String dateStr) {
		// TODO Auto-generated method stub
		return null;
	}

	private static String getDay(List<Object> yearOrMonthOrDayOrSeasonOrMedlineDate, List<ArticleDate> articleDate) {
		for (Object obj : yearOrMonthOrDayOrSeasonOrMedlineDate) {
			if (obj instanceof MedlineDate) {
				MedlineDate md = (MedlineDate) obj;
				String dateStr = md.getvalue();
				String day = getDay(dateStr);
				if (day != null) {
					return day;
				}
			} else if (obj instanceof Day) {
				Day d = (Day) obj;
				if (d != null) {
					return d.getvalue();
				}
			}
		}

		for (ArticleDate ad : articleDate) {
			Day d = ad.getDay();
			if (d != null) {
				return d.getvalue();
			}
		}
		return null;
	}

	private static String getDay(String dateStr) {
		// TODO Auto-generated method stub
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
