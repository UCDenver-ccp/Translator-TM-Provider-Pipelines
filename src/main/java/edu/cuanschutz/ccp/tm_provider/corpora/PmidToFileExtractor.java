package edu.cuanschutz.ccp.tm_provider.corpora;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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

import org.medline.DeleteCitation;
import org.medline.MedlineCitation;
import org.medline.PMID;
import org.medline.PubmedArticle;
import org.medline.PubmedArticleSet;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil.FileSuffixEnforcement;
import edu.ucdenver.ccp.common.file.FileWriterUtil.WriteMode;

/**
 * Extracts PMIDs from Medline XML files and outputs a 2-column file linking
 * PMID to file name
 *
 */
public class PmidToFileExtractor {

	protected static void extractPmids(InputStream xmlStream, File outputDirectory, String inputFileName)
			throws JAXBException, XMLStreamException, IOException {

		File pmidsOutputFile = new File(outputDirectory, inputFileName + ".ids");
		File deletedPmidsOutputFile = new File(outputDirectory, inputFileName + ".deleted.ids");

		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(pmidsOutputFile, CharacterEncoding.UTF_8,
				WriteMode.APPEND, FileSuffixEnforcement.OFF);
				BufferedWriter deletedPmidWriter = FileWriterUtil.initBufferedWriter(deletedPmidsOutputFile,
						CharacterEncoding.UTF_8, WriteMode.APPEND, FileSuffixEnforcement.OFF)) {

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
				writer.write(String.format("%s\t%s\n", pmid, inputFileName));

				while (reader.hasNext()
						&& (!reader.isStartElement() || !reader.getLocalName().equals("PubmedArticle"))) {
					reader.next();
					if (reader.isStartElement() && reader.getLocalName().equals("DeleteCitation")) {
						JAXBElement<DeleteCitation> deleteElement = um.unmarshal(reader, DeleteCitation.class);
						DeleteCitation dc = deleteElement.getValue();
						List<PMID> pmidsToBeDeleted = dc.getPMID();
						for (PMID pmidToDelete : pmidsToBeDeleted) {
							deletedPmidWriter.write(String.format("%s\t%s\n", pmidToDelete.getvalue(), inputFileName));
						}
					}
				}
			}
			reader.close();
		}
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
						extractPmids(xmlStream, outputDirectory, file.getName());
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
