package edu.cuanschutz.ccp.tm_provider.etl.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLResolver;
import javax.xml.stream.XMLStreamException;

import com.pengyifan.bioc.BioCDocument;
import com.pengyifan.bioc.BioCPassage;
import com.pengyifan.bioc.io.BioCDocumentReader;

import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;
import lombok.Data;

public class BiocToTextConverter {

	public static final String PLAIN_TEXT_KEY = "text";
	public static final String SECTION_ANNOTATIONS_KEY = "annots";
	/**
	 * a set of sections that can break other larger sections (where you would
	 * expect the other section to possibly continue)
	 */
	private static Set<String> breakingSections = populateBreakingSectionSet();
	/**
	 * a list of known section names; unknown section names will be flagged (or may
	 * provoke an exception)
	 */
	private static Set<String> knownSections = populateKnownSectionsSet();

	private static Logger logger = Logger.getLogger(BiocToTextConverter.class.getName());

	public static Map<String, TextDocument> convert(InputStream input)
			throws FactoryConfigurationError, XMLStreamException, IOException {
		String text = "";

		Map<String, TextDocument> docId2DocumentMap = new HashMap<String, TextDocument>();
		try (BioCDocumentReader reader = new BioCDocumentReader(input, getBioCXmlResolver())) {
			String source = reader.readCollectionInfo().getSource();
			BioCDocument doc = null;
			while ((doc = reader.readDocument()) != null) {
				String docId = source + doc.getID();
				TextAnnotationFactory taFactory = TextAnnotationFactory.createFactoryWithDefaults(docId);

				List<TextAnnotation> sections = new ArrayList<TextAnnotation>();
				Stack<OpenSection> openSections = new Stack<OpenSection>();

				text = processBioCDocument(text, doc, sections, openSections, taFactory);

				/*
				 * close any remaining open sections - likely should just be the reference
				 * section at this point.
				 */
				while (!openSections.isEmpty()) {
					closeSection(text, sections, openSections, taFactory);
				}

				TextDocument td = new TextDocument(docId, source, text);
				td.addAnnotations(sections);
				docId2DocumentMap.put(docId, td);
//				System.out.println(text);
			}
		}
		return docId2DocumentMap;
	}

	/**
	 * This method thanks to:
	 * https://stackoverflow.com/questions/10685668/how-to-load-a-relative-system-dtd-into-a-stax-parser
	 * 
	 * @return
	 */
	private static XMLResolver getBioCXmlResolver() {
		return new XMLResolver() {

			@Override
			public Object resolveEntity(String publicID, String systemID, String baseURI, String namespace)
					throws XMLStreamException {

				/*
				 * The systemID argument is the same dtd file specified in the xml file header.
				 * For example, if the xml header is <!DOCTYPE dblp SYSTEM "dblp.dtd">, then
				 * systemID will be "dblp.dtd".
				 * 
				 */
				return Thread.currentThread().getContextClassLoader().getResourceAsStream(systemID);

			}
		};
	}

	private static Set<String> populateBreakingSectionSet() {
		Set<String> set = new HashSet<String>();
		set.add("FIG");
		set.add("TABLE");
		return set;
	}

	private static Set<String> populateKnownSectionsSet() {
		Set<String> set = new HashSet<String>();
		set.add("TITLE");
		set.add("ABSTRACT");
		set.add("INTRO");
		set.add("FIG");
		set.add("RESULTS");
		set.add("DISCUSS");
		set.add("METHODS");
		set.add("REF");
		set.add("ABBR");
		set.add("ACK_FUND");
		set.add("APPENDIX");
		set.add("AUTH_CONT");
		set.add("CASE");
		set.add("COMP_INT");
		set.add("CONCL");
		set.add("KEYWORD");
		set.add("REVIEW_INFO");
		set.add("SUPPL");
		return set;
	}

	/**
	 * process each passage in a BioC document. Return the plain text of the
	 * document with passage byte offsets corresponding to those stipulated by the
	 * document. During processing, create section annotations for main sections,
	 * e.g. INTRO, RESULTS, etc., as well as for paragraphs, section headings, etc.
	 * 
	 * @param text
	 * @param doc
	 * @param sections
	 * @param openSections
	 * @param taFactory
	 * @return
	 */
	private static String processBioCDocument(String text, BioCDocument doc, List<TextAnnotation> sections,
			Stack<OpenSection> openSections, TextAnnotationFactory taFactory) {
		for (BioCPassage passage : doc.getPassages()) {

			String passageType = passage.getInfon("type").get();
			if (passage.getText().isPresent()) {
				sections.add(getPassageAnnotation(text, passage.getText().get(), passageType, taFactory));

				String sectionType = passage.getInfon("section_type").get();
				if (!(knownSections.contains(sectionType) || breakingSections.contains(sectionType))) {
					logger.log(Level.WARNING,
							"Unknown section type observed: " + sectionType + " in document: " + doc.getID());
				}
				updateSectionTypes(text, sections, openSections, sectionType, taFactory);

				text = updateText(text, doc, passage);
			} else {
				// encountered passage with no text
			}
		}
		return text;
	}

	/**
	 * updates the openSections stack when a new section is started. Added
	 * SectionAnnotations to the sections list when a section is closed.
	 * 
	 * @param text
	 * @param sections
	 * @param openSections
	 * @param sectionName
	 * @param taFactory
	 */
	private static void updateSectionTypes(String text, List<TextAnnotation> sections, Stack<OpenSection> openSections,
			String sectionName, TextAnnotationFactory taFactory) {
		if (openSections.isEmpty()) {
			openSection(text, openSections, sectionName);
		} else {
			String prevSectionName = openSections.peek().getType();
			if (!sectionName.equals(prevSectionName)) {
				if (!breakingSections.contains(sectionName)) {
					/*
					 * if not a breaking section, i.e. we don't expect the current section to
					 * continue afterward, then we are closing the old section and opening a new
					 * section. Otherwise, just open a new section.
					 */
					closeSection(text, sections, openSections, taFactory);
				}
				if (breakingSections.contains(prevSectionName) && openSections.peek().getType().equals(sectionName)) {
					/*
					 * do nothing - we are continuing in the same major section
					 */
				} else if (breakingSections.contains(prevSectionName)
						&& !openSections.peek().getType().equals(sectionName)) {
					/*
					 * we just closed a breaking section, but the next section is different from the
					 * one that was interrupted by the breaking section, so we pop and close the top
					 * section, and then we open a new section.
					 */
					closeSection(text, sections, openSections, taFactory);
					openSection(text, openSections, sectionName);
				} else {
					openSection(text, openSections, sectionName);
				}
			}
		}
	}

	/**
	 * creates a section annotation for the specific section type of the passage,
	 * e.g. paragraph, section_heading, etc.
	 * 
	 * @param text
	 * @param sections
	 * @param passage
	 * @param sectionType
	 */
	private static TextAnnotation getPassageAnnotation(String text, String passageText, String sectionType,
			TextAnnotationFactory taFactory) {
		if (sectionType.equals("abstract")) {
			sectionType = "paragraph";
		} else if (sectionType.equals("ref")) {
			sectionType = "reference";
		}
		if (sectionType.startsWith("title")) {
			sectionType = sectionType.replace("title", "section_heading");
		}
		return taFactory.createAnnotation(text.length(), text.length() + passageText.length(), passageText,
				sectionType);
	}

	/**
	 * pop and open section from the stack and close it, i.e. create a
	 * SectionAnnotation
	 * 
	 * @param text
	 * @param sections
	 * @param openSections
	 */
	private static void closeSection(String text, List<TextAnnotation> sections, Stack<OpenSection> openSections,
			TextAnnotationFactory factory) {
		OpenSection sectionToClose = openSections.pop();
		int startOffset = sectionToClose.getStartOffset();
		int endOffset = text.length();
		TextAnnotation sectionAnnot = factory.createAnnotation(startOffset, endOffset,
				text.substring(startOffset, endOffset), sectionToClose.getType());
		sections.add(sectionAnnot);
	}

	/**
	 * create a new OpenSection and push it to the stack
	 * 
	 * @param text
	 * @param openSections
	 * @param sectionName
	 */
	private static void openSection(String text, Stack<OpenSection> openSections, String sectionName) {
		openSections.push(new OpenSection(text.length(), sectionName));
	}

	/**
	 * build the text by adding the current passage text to it, accounting for
	 * changes needed to keep it aligned with the offsets specified by the BioC
	 * document.
	 * 
	 * @param text
	 * @param doc
	 * @param passage
	 * @return
	 */
	private static String updateText(String text, BioCDocument doc, BioCPassage passage) {
		text = matchTextByteOffsetToBioCByteOffset(doc, text, passage.getOffset());
		/* each passage is by default separated by a line break */
		text += (((!text.isEmpty()) ? "\n" : "") + passage.getText().get());
		return text;
	}

	/**
	 * The BioC files stipulate a byte offset for each passage. When converting the
	 * BioC documents to plain text, we must ensure that the plain text byte offsets
	 * match those that are provided by the BioC documents. Aligning the offsets is
	 * achieved by adding line breaks to the text.
	 * 
	 * @param doc
	 * @param text
	 * @param passage
	 * @return
	 */
	private static String matchTextByteOffsetToBioCByteOffset(BioCDocument doc, String text, int passageByteOffset) {

		/*
		 * we assume that the text byteoffset is equal to or less than the offset
		 * stipulated by the BioC document. If this is not the case, then throw an
		 * exception because this code is unable to recover from such a situation.
		 */
		int byteCount = text.getBytes().length;
		if (byteCount > passageByteOffset) {
			throw new IllegalStateException(
					"Unable to convert document to plain text due to byte offset issue: " + doc.getID());
		}

		String updatedText = text;
		while (passageByteOffset > byteCount) {
			updatedText += "\n";
			byteCount = updatedText.getBytes().length;
		}

		return updatedText;
	}

	/**
	 * Simple data structure to store section start offsets as a document is
	 * processed
	 *
	 */
	@Data
	private static class OpenSection {
		private final int startOffset;
		private final String type;
	}

}
