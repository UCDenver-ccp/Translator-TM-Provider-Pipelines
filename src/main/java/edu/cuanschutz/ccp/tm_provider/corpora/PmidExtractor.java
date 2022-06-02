package edu.cuanschutz.ccp.tm_provider.corpora;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.beam.vendor.grpc.v1p26p0.com.jcraft.jzlib.GZIPInputStream;
import org.medline.AbstractText;
import org.medline.MedlineCitation;
import org.medline.PubmedArticle;
import org.medline.PubmedArticleSet;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil.FileSuffixEnforcement;
import edu.ucdenver.ccp.common.file.FileWriterUtil.WriteMode;

public class PmidExtractor {

	private static void extractPmids(InputStream xmlStream, File outputFile)
			throws JAXBException, XMLStreamException, IOException {

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

			// read a book at a time
			while (reader.getEventType() == XMLStreamConstants.START_ELEMENT) {
				JAXBElement<PubmedArticle> element = um.unmarshal(reader, PubmedArticle.class);
				PubmedArticle article = element.getValue();

				MedlineCitation medlineCitation = article.getMedlineCitation();

				String pmid = medlineCitation.getPMID().getvalue();

				if (!abstractOrTitleContainsHtml(medlineCitation)) {
					writer.write(pmid + "\n");
					System.out.println("LOGGING: " + pmid);
				} else {
					System.out.println("PMID HAS HTML: " + pmid);
				}

//				if (reader.getEventType() == XMLStreamConstants.CHARACTERS) {
//					reader.next();
//				}
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

		try {
			File xmlDirectory = new File(args[0]);
			File outputFile = new File(args[1]);

			for (Iterator<File> fileIterator = FileUtil.getFileIterator(xmlDirectory, false, ".gz"); fileIterator
					.hasNext();) {
				File file = fileIterator.next();
				System.out.println("Processing: " + file.getAbsolutePath());
				InputStream xmlStream = new GZIPInputStream(new FileInputStream(file));
				extractPmids(xmlStream, outputFile);
			}
		} catch (JAXBException | XMLStreamException | IOException e) {
			e.printStackTrace();
		}

	}
}
