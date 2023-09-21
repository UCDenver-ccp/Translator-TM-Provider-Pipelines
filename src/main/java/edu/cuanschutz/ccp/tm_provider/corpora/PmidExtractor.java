package edu.cuanschutz.ccp.tm_provider.corpora;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBElement;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Unmarshaller;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.medline.AbstractText;
import org.medline.MedlineCitation;
import org.medline.PubmedArticle;
import org.medline.PubmedArticleSet;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil.FileSuffixEnforcement;
import edu.ucdenver.ccp.common.file.FileWriterUtil.WriteMode;

/**
 * Extracts PMIDs from Medline XML files based on the DocumentTarget flag -
 * either if the title/abstract contain HTML or not
 *
 */
public class PmidExtractor {

	public enum DocumentTarget {
		CONTAINS_HTML, DOES_NOT_CONTAIN_HTML
	}

	protected static void extractPmids(InputStream xmlStream, File outputDirectory, String inputFileName,
			DocumentTarget target) throws JAXBException, XMLStreamException, IOException {

		File outputFile = new File(outputDirectory, inputFileName + "." + target.name().toLowerCase() + ".ids");

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

				if (target == DocumentTarget.CONTAINS_HTML) {
					if (abstractOrTitleContainsHtml(medlineCitation)) {
						writer.write(pmid + "\n");
					}
				} else if (target == DocumentTarget.DOES_NOT_CONTAIN_HTML) {
					if (!abstractOrTitleContainsHtml(medlineCitation)) {
						writer.write(pmid + "\n");
					}
				}

				while (reader.hasNext()
						&& (!reader.isStartElement() || !reader.getLocalName().equals("PubmedArticle"))) {
					reader.next();
				}

			}

			reader.close();

		}

	}

	private static boolean abstractOrTitleContainsHtml(MedlineCitation medlineCitation) {
		List<String> htmlCodes = Arrays.asList("<b>", "<i>", "<u>", "<sub>", "<sup>");

		String title = medlineCitation.getArticle().getArticleTitle().getvalue();

		for (String html : htmlCodes) {
			if (title.contains(html)) {
				return true;
			}
		}

		if (medlineCitation.getArticle().getAbstract() != null) {
			for (AbstractText text : medlineCitation.getArticle().getAbstract().getAbstractText()) {
				for (String html : htmlCodes) {
					if (text.getvalue().contains(html)) {
						return true;
					}
				}
			}
		}

		return false;
	}

	public static void main(String[] args) {

		File xmlDirectory = new File(args[0]);
		File outputDirectory = new File(args[1]);
		DocumentTarget target = DocumentTarget.valueOf(args[2]);
		int skip = Integer.parseInt(args[3]);
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
						extractPmids(xmlStream, outputDirectory, file.getName(), target);
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
