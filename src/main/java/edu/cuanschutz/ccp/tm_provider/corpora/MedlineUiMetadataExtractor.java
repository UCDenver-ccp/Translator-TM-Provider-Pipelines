package edu.cuanschutz.ccp.tm_provider.corpora;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBElement;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Unmarshaller;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.medline.ArticleDate;
import org.medline.Day;
import org.medline.DeleteCitation;
import org.medline.MedlineCitation;
import org.medline.MedlineDate;
import org.medline.Month;
import org.medline.PMID;
import org.medline.PubmedArticle;
import org.medline.PubmedArticleSet;
import org.medline.Season;
import org.medline.Year;

import edu.cuanschutz.ccp.tm_provider.etl.fn.MedlineXmlToTextFn;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;

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
				String month = getMonth(yearOrMonthOrDayOrSeasonOrMedlineDate, articleDate, pmid);
				String day = getDay(yearOrMonthOrDayOrSeasonOrMedlineDate, articleDate, pmid);
				String journalNameAbbreviation = medlineCitation.getArticle().getJournal().getISOAbbreviation();
				String journalName = medlineCitation.getArticle().getJournal().getTitle();
				String journalIssue = medlineCitation.getArticle().getJournal().getJournalIssue().getIssue();
				String journalVolume = medlineCitation.getArticle().getJournal().getJournalIssue().getVolume();
				String articleTitle = MedlineXmlToTextFn.extractTitleText(medlineCitation,
						new ArrayList<TextAnnotation>());
				String abstractText = MedlineXmlToTextFn.getAbstractText(article, new ArrayList<TextAnnotation>());

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

	private static String getMonth(List<Object> yearOrMonthOrDayOrSeasonOrMedlineDate, List<ArticleDate> articleDate,
			String pmid) {
		for (Object obj : yearOrMonthOrDayOrSeasonOrMedlineDate) {
			if (obj instanceof MedlineDate) {
				MedlineDate md = (MedlineDate) obj;
				String dateStr = md.getvalue();
				String month = getMonth(dateStr, pmid);
				if (month != null) {
					return month;
				}
			} else if (obj instanceof Month) {
				Month m = (Month) obj;
				if (m != null) {
					String val = m.getvalue();
					return getThreeLetterAbbrev(val);
				}
			} else if (obj instanceof Season) {
				Season s = (Season) obj;
				String season = s.getvalue();
				if (season.equalsIgnoreCase("winter")) {
					return "Jan";
				}
				if (season.equalsIgnoreCase("spring")) {
					return "Apr";
				}
				if (season.equalsIgnoreCase("summer")) {
					return "Jul";
				}
				if (season.equalsIgnoreCase("fall")) {
					return "Oct";
				}
				if (season.equalsIgnoreCase("autumn")) {
					return "Oct";
				}
			}
		}

		for (ArticleDate ad : articleDate) {
			Month m = ad.getMonth();
			if (m != null) {
				return getThreeLetterAbbrev(m.getvalue());
			}
		}
		return null;
	}

	private static String getThreeLetterAbbrev(String month) {

		switch (month) {
		case "1":
			return "Jan";
		case "2":
			return "Feb";
		case "3":
			return "Mar";
		case "4":
			return "Apr";
		case "5":
			return "May";
		case "6":
			return "Jun";
		case "7":
			return "Jul";
		case "8":
			return "Aug";
		case "9":
			return "Sep";
		case "01":
			return "Jan";
		case "02":
			return "Feb";
		case "03":
			return "Mar";
		case "04":
			return "Apr";
		case "05":
			return "May";
		case "06":
			return "Jun";
		case "07":
			return "Jul";
		case "08":
			return "Aug";
		case "09":
			return "Sep";
		case "10":
			return "Oct";
		case "11":
			return "Nov";
		case "12":
			return "Dec";

		case "Jan":
			return "Jan";
		case "Feb":
			return "Feb";
		case "Mar":
			return "Mar";
		case "Apr":
			return "Apr";
		case "May":
			return "May";
		case "Jun":
			return "Jun";
		case "Jul":
			return "Jul";
		case "Aug":
			return "Aug";
		case "Sep":
			return "Sep";
		case "Oct":
			return "Oct";
		case "Nov":
			return "Nov";
		case "Dec":
			return "Dec";

		default:
			System.err.println("XXXXXX unable to convert to 3-letter month: " + month);
			return null;
//			throw new IllegalArgumentException("unable to convert to 3-letter month: " + month);
		}
	}

	/**
	 * Look for first 3-letter abbreviation and return that as the month. Also
	 * converts things like seasons and 1st quarter to an approximate month.
	 * 
	 * @param dateStr
	 * @param pmid
	 * @return
	 */
	protected static String getMonth(String dateStr, String pmid) {
		String month = extractExplicitlyMentionedMonth(dateStr);
		if (month != null) {
			return month;
		}

		month = checkJanPatterns(dateStr);
		if (month != null) {
			return month;
		}

		month = checkAprPatterns(dateStr);
		if (month != null) {
			return month;
		}

		month = checkJulPatterns(dateStr);
		if (month != null) {
			return month;
		}

		month = checkOctPatterns(dateStr);
		if (month != null) {
			return month;
		}

		// @formatter:off
		List<String> noMonthPatterns = Arrays.asList(
				"^\\d\\d\\d\\d ?-\\d\\d\\d\\d$",
				"^\\d\\d\\d\\d( \\d+-\\d+)?$");
		// @formatter:on
		for (String pattern : noMonthPatterns) {
			Pattern p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
			Matcher m = p.matcher(dateStr);
			if (m.find()) {
				return null;
			}
		}

		System.out.println("returning null month from: " + dateStr);
		return null;

	}

	private static String checkOctPatterns(String dateStr) {
		// @formatter:off
		List<String> octPatterns = Arrays.asList(
				"^\\d\\d\\d\\d (4th)|(Fourth)|(4d) Quart(er)?$",
				"^\\d\\d\\d\\d Fall([-/]\\w+)?( 01)?$",
				"^\\d\\d\\d\\d Autumn([-/]\\w+)?( 01)?$",
				"^\\d\\d\\d\\d Fall(-\\d\\d\\d\\d \\w+)?$",
				"^\\d\\d\\d\\d Autumn(-\\d\\d\\d\\d \\w+)?$",
				"^Fall \\d\\d\\d\\d$",
				"^Autumn \\d\\d\\d\\d$",
				"^\\d\\d\\d\\d-\\d\\d\\d\\d Fall(-\\w+)?$",
				"^\\d\\d\\d\\d-\\d\\d\\d\\d Autumn(-\\w+)?$");
		// @formatter:on
		for (String pattern : octPatterns) {
			Pattern p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
			Matcher m = p.matcher(dateStr);
			if (m.find()) {
				return "Oct";
			}
		}
		return null;
	}

	private static String checkJulPatterns(String dateStr) {
		// @formatter:off
		List<String> julPatterns = Arrays.asList(
				"^\\d\\d\\d\\d (3rd)|(Third)|(3d) Quart(er)?$",
				"^\\d\\d\\d\\d Summer([-/]\\w+)?( 01)?$",
				"^\\d\\d\\d\\d Summer(-\\d\\d\\d\\d \\w+)?$",
				"^Summer \\d\\d\\d\\d$",
				"^\\d\\d\\d\\d-\\d\\d\\d\\d Summer(-\\w+)?$");
		// @formatter:on
		for (String pattern : julPatterns) {
			Pattern p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
			Matcher m = p.matcher(dateStr);
			if (m.find()) {
				return "Jul";
			}
		}
		return null;
	}

	private static String checkAprPatterns(String dateStr) {
		// @formatter:off
		List<String> aprPatterns = Arrays.asList(
				"^\\d\\d\\d\\d (2nd)|(Second)|(2d) Quart(er)?$",
				"^\\d\\d\\d\\d Spring([-/]\\w+)?( 01)?$",
				"^\\d\\d\\d\\d Spring(-\\d\\d\\d\\d \\w+)?$",
				"^Spring \\d\\d\\d\\d$",
				"^\\d\\d\\d\\d-\\d\\d\\d\\d Spring(-\\w+)?$");
		// @formatter:on
		for (String pattern : aprPatterns) {
			Pattern p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
			Matcher m = p.matcher(dateStr);
			if (m.find()) {
				return "Apr";
			}
		}
		return null;
	}

	private static String checkJanPatterns(String dateStr) {
		// @formatter:off
		List<String> janPatterns = Arrays.asList(
				"^\\d\\d\\d\\d (1st)|(First)|(1d) Quart(er)?$",
				"^\\d\\d\\d\\d Winter(-\\w+)?( 01)?$",
				"^\\d\\d\\d\\d Winter(-\\d\\d\\d\\d \\w+)?$",
				"^Winter \\d\\d\\d\\d$",
				"^\\d\\d\\d\\d-\\d\\d\\d\\d Winter(-\\w+)?$");
		// @formatter:on
		for (String pattern : janPatterns) {
			Pattern p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
			Matcher m = p.matcher(dateStr);
			if (m.find()) {
				return "Jan";
			}
		}
		return null;
	}

	private static String extractExplicitlyMentionedMonth(String dateStr) {
		Pattern p = Pattern.compile(
				"(Jan)|(Feb)|(Mar)|(Apr)|(May)|(Jun)|(Jul)|(Aug)|(Sep)|(Oct)|(Nov)|(Dec)|(janvier)|(février)|(mars)|(avril)|(mai)|(juin)|(juillet)|(aout)|(septembre)|(octobre)|(novembre)|(décembre)",
				Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(dateStr);
		if (m.find()) {
			String month = m.group().toLowerCase();
			month = month.substring(0, 1).toUpperCase() + month.substring(1);
			if (month.equalsIgnoreCase("janvier")) {
				month = "Jan";
			} else if (month.equalsIgnoreCase("février")) {
				month = "Feb";
			} else if (month.equalsIgnoreCase("mars")) {
				month = "Mar";
			} else if (month.equalsIgnoreCase("avril")) {
				month = "Apr";
			} else if (month.equalsIgnoreCase("mai")) {
				month = "May";
			} else if (month.equalsIgnoreCase("juin")) {
				month = "Jun";
			} else if (month.equalsIgnoreCase("juillet")) {
				month = "Jul";
			} else if (month.equalsIgnoreCase("aout")) {
				month = "Aug";
			} else if (month.equalsIgnoreCase("septembre")) {
				month = "Sep";
			} else if (month.equalsIgnoreCase("octobre")) {
				month = "Oct";
			} else if (month.equalsIgnoreCase("novembre")) {
				month = "Nov";
			} else if (month.equalsIgnoreCase("décembre")) {
				month = "Dec";
			}
			return month;
		}
		return null;
	}

	private static String getDay(List<Object> yearOrMonthOrDayOrSeasonOrMedlineDate, List<ArticleDate> articleDate,
			String pmid) {
		for (Object obj : yearOrMonthOrDayOrSeasonOrMedlineDate) {
			if (obj instanceof MedlineDate) {
				MedlineDate md = (MedlineDate) obj;
				String dateStr = md.getvalue();
				String day = getDay(dateStr, pmid);
				if (day != null) {
					return enforceTwoDigitDay(day);
				}
			} else if (obj instanceof Day) {
				Day d = (Day) obj;
				if (d != null) {
					return enforceTwoDigitDay(d.getvalue());
				}
			}
		}

		for (ArticleDate ad : articleDate) {
			Day d = ad.getDay();
			if (d != null) {
				return enforceTwoDigitDay(d.getvalue());
			}
		}
		return null;
	}

	/**
	 * Forces the day to be two-digits, e.g. 03
	 * 
	 * @param getvalue
	 * @return
	 */
	private static String enforceTwoDigitDay(String day) {
		if (day.length() == 1) {
			return "0" + day;
		}
		return day;
	}

	protected static String getDay(String dateStr, String pmid) {
		List<String> patternsWhereNoDayIsPresent = Arrays.asList(
				"^\\d\\d\\d\\d (Jan)|(Feb)|(Mar)|(Apr)|(May)|(Jun)|(Jul)|(Aug)|(Sep)|(Oct)|(Nov)|(Dec)-(Jan)|(Feb)|(Mar)|(Apr)|(May)|(Jun)|(Jul)|(Aug)|(Sep)|(Oct)|(Nov)|(Dec)$",
				"^\\d\\d\\d\\d (Spring)|(Summer)|(Winter)|(Fall)|(Autumn)$",
				"^(Spring)|(Summer)|(Winter)|(Fall)|(Autumn) \\d\\d\\d\\d$",
				"^\\d\\d\\d\\d (1st Quarter)|(2nd Quarter)|(3rd Quarter)|(4th Quarter)$",
				"^\\d\\d\\d\\d (Jan)|(Feb)|(Mar)|(Apr)|(May)|(Jun)|(Jul)|(Aug)|(Sep)|(Oct)|(Nov)|(Dec)-\\d\\d\\d\\d (Jan)|(Feb)|(Mar)|(Apr)|(May)|(Jun)|(Jul)|(Aug)|(Sep)|(Oct)|(Nov)|(Dec)$",
				"^\\d\\d\\d\\d( \\d+-\\d+)?$", "^\\d\\d\\d\\d ?-\\d\\d\\d\\d( Winter)?$",
				"^\\d\\d\\d\\d (Jan)|(Feb)|(Mar)|(Apr)|(May)|(Jun)|(Jul)|(Aug)|(Sep)|(Oct)|(Nov)|(Dec)|(Mai)$");

		for (String pattern : patternsWhereNoDayIsPresent) {
			if (Pattern.compile(pattern, Pattern.CASE_INSENSITIVE).matcher(dateStr).matches()) {
				// there is no day listed in these patterns so we return null
				return null;
			}
		}

		// this one pattern does contain a day, so we extract it and return it.
		Pattern p = Pattern.compile(
				"^\\d\\d\\d\\d (Jan)|(Feb)|(Mar)|(Apr)|(May)|(Jun)|(Jul)|(Aug)|(Sep)|(Oct)|(Nov)|(Dec) (\\d+)(-\\d+)?$",
				Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(dateStr);
		if (m.find()) {
			return m.group(13);
		}

		System.out.println("returning null day from: " + dateStr);
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
