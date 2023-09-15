package edu.ucdenver.ccp.file.conversion.bionlp;

/*-
 * #%L
 * Colorado Computational Pharmacology's file conversion
 * 						project
 * %%
 * Copyright (C) 2019 Regents of the University of Colorado
 * %%
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 * 
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * 3. Neither the name of the Regents of the University of Colorado nor the names of its contributors
 *    may be used to endorse or promote products derived from this software without
 *    specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import edu.ucdenver.ccp.common.io.StreamUtil;
import edu.ucdenver.ccp.common.string.RegExPatterns;
import edu.ucdenver.ccp.file.conversion.DocumentReader;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.util.DocumentReaderUtil;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationUtil;
import edu.ucdenver.ccp.nlp.core.mention.impl.DefaultClassMention;

public class BioNLPDocumentReader extends DocumentReader {

	private static final Logger logger = Logger.getLogger(BioNLPDocumentReader.class);

	/**
	 * The BioNLP format does not support spaces in annotation and relation types,
	 * so any spaces in an annotation or relation type must be replaced. This
	 * constant is used as a replacement in the BioNLP format documents created by
	 * this DocumentWriter.
	 */
	private final String spacePlaceholder;

	public BioNLPDocumentReader(String spacePlaceholder) {
		super();
		this.spacePlaceholder = spacePlaceholder;
	}

	public BioNLPDocumentReader() {
		super();
		this.spacePlaceholder = BioNLPDocumentWriter.SPACE_PLACEHOLDER;
	}

	public static final String THEME_ID_SLOT_NAME = "theme id";

	@Override
	public TextDocument readDocument(String sourceId, String sourceDb, InputStream inputStream,
			InputStream documentTextStream, CharacterEncoding encoding) throws IOException {
		String documentText = StreamUtil.toString(new InputStreamReader(documentTextStream, encoding.getDecoder()));

		TextDocument td = new TextDocument(sourceId, sourceDb, documentText);

		Map<String, TextAnnotation> annotIdToAnnotMap = new HashMap<String, TextAnnotation>();
		/* extract theme annotations */
		Set<String> relationLines = new HashSet<String>();
		extractThemeAnnotations(sourceId, inputStream, encoding, documentText, annotIdToAnnotMap, relationLines);
		/* process relation annotations */
		processRelationAnnotations(relationLines, encoding, annotIdToAnnotMap);
		/* add annotations to document */

		Collection<TextAnnotation> annotations = annotIdToAnnotMap.values();
		DocumentReaderUtil.validateSpans(annotations, documentText, sourceId);
		td.addAnnotations(annotations);

		return td;
	}

	public void processRelationAnnotations(Set<String> relationLines, CharacterEncoding encoding,
			Map<String, TextAnnotation> annotIdToAnnotMap) throws IOException {
		for (String line : relationLines) {
			if (line.startsWith("E") || line.startsWith("M")) {
				throw new IllegalStateException(
						"Event and modification annotations are not handled by this DocumentReader. Code modifications required.");
			}
			if (line.startsWith("R")) {
				String[] toks = line.split("\\t");
				@SuppressWarnings("unused")
				String relationId = toks[0];

				Pattern p = Pattern.compile("(.*?) Arg1:(.*?) Arg2:(.*?)$");
				Matcher m = p.matcher(toks[1]);
				if (m.find()) {
					String relationType = m.group(1);

					if (spacePlaceholder != null) {
						relationType = relationType.replaceAll(RegExPatterns.escapeCharacterForRegEx(spacePlaceholder),
								" ");
					}

					String annotId1 = m.group(2);
					String annotId2 = m.group(3);

					TextAnnotation sourceTa = annotIdToAnnotMap.get(annotId1);
					TextAnnotation targetTa = annotIdToAnnotMap.get(annotId2);

					if (sourceTa == null) {
						System.err.println("Missing annotation for annot id: " + annotId1
								+ " -- Most likely an annotation with discontinous spans which are not currentl handled.");
						continue;
					}

					if (targetTa == null) {
						System.err.println("Missing annotation for annot id: " + annotId2
								+ " -- Most likely an annotation with discontinous spans which are not currentl handled.");
						continue;
					}

					DocumentReader.createAnnotationRelation(sourceTa, targetTa, relationType);
				} else {
					throw new IllegalStateException("Unable to parse relation from line: " + line);
				}
			}

		}
	}

	public void extractThemeAnnotations(String sourceId, InputStream inputStream, CharacterEncoding encoding,
			String documentText, Map<String, TextAnnotation> annotIdToAnnotMap, Set<String> relationLines)
			throws IOException {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults(sourceId);
		StreamLineIterator lineIter = new StreamLineIterator(inputStream, encoding, null);
		while (lineIter.hasNext()) {
			String line = lineIter.next().getText();
			if (line.startsWith("T")) {
				extractAnnotations(documentText, annotIdToAnnotMap, factory, line);
			} else {
				relationLines.add(line);
			}
		}
		lineIter.close();
	}

	private boolean isValidAnnotationLine(String line) {
		String[] toks = line.split("\\t");
		if (toks.length != 3) {
			logger.warn("Invalid annotation line detected (column count != 3): " + line);
			return false;
		}
		if (!toks[0].matches("T\\d+")) {
			logger.warn("Invalid annotation line detected (first column does not match T\\d+): " + line);
			return false;
		}
		String[] mid = toks[1].split(" ");
		if (mid.length != 3) {
			logger.warn("Invalid annotation line detected (middle column does not have 3 columns): " + line);
			return false;
		}
		return true;
	}

	private void extractAnnotations(String documentText, Map<String, TextAnnotation> annotIdToAnnotMap,
			TextAnnotationFactory factory, String line) {
		if (isValidAnnotationLine(line)) {

			String[] toks = line.split("\\t");
			String annotId = toks[0];

			String annotType = toks[1].substring(0, toks[1].indexOf(" "));
			/* replace all spaceHolders in annotation type with spaces */
			if (spacePlaceholder != null) {
				annotType = annotType.replaceAll(RegExPatterns.escapeCharacterForRegEx(spacePlaceholder), " ");
			}
			String spanStr = toks[1].substring(toks[1].indexOf(" ") + 1);

			TextAnnotation ta = null;
			// TODO FIX SPAN VALIDATION WHEN THERE IS A DISCONTINUOUS SPAN -- will involve
			// parsing the expected covered text
			String coveredText = ""; // expected covered text will not appear in the document
			if (toks.length > 2) {
				coveredText = toks[2]; // TODO: Note that if this is a discontinuous span, then the
			}
			String[] spanToks = spanStr.split(";");

			for (String spanTok : spanToks) {
				String[] spanTokToks = spanTok.split(" ");
				int spanStart = Integer.parseInt(spanTokToks[0]);
				int spanEnd = Integer.parseInt(spanTokToks[1]);
				if (ta == null) {
					ta = factory.createAnnotation(spanStart, spanEnd, "", new DefaultClassMention(annotType));
					TextAnnotationUtil.addSlotValue(ta, THEME_ID_SLOT_NAME, annotId);

//						// TODO: NOTE - validation of spans/covered text needs to be implemented for
//						// discontinuous span annots
//						if (spanToks.length == 1) {
//							// validate covered text/spans
//							String ct = documentText.substring(spanStart, spanEnd).replaceAll("\\n", " ");
//							if (!ct.equals(expectedCoveredText)) {
//
//								System.out.println("expected: " + expectedCoveredText);
//								System.out.println("but was :" + ct);
//
//								boolean fixed = false;
//								// if the covered text is not as expected, then shift the spans slightly to see
//								// if a match can be found
//								int rewind = (spanStart > 15) ? -15 : (-1 * spanStart);
//								int ff = (spanEnd + 15 < documentText.length()) ? 15
//										: (documentText.length() - spanEnd - 1);
//								for (int r = rewind; r < ff; r++) {
//									ct = documentText.substring(spanStart+r, spanEnd+r);
//									System.out.println("CT: " + ct + " r: " + r);
//									if (ct.equals(expectedCoveredText)) {
//										ta.setAnnotationSpanStart(spanStart+r);
//										ta.setAnnotationSpanEnd(spanEnd+r);
//										fixed = true;
//										coveredText = expectedCoveredText;
//										break;
//									}
//								}
//
//								if (!fixed) {
//									throw new RuntimeException("Span mismatch!! Expected: '" + ct + "' but was '"
//											+ expectedCoveredText + "'");
//								}
//							}
//						}
					// TODO -- revisit for use with discontinuous annotations
					if (coveredText.isEmpty() && documentText.length() > ta.getAnnotationSpanEnd()) {
						coveredText = documentText.substring(ta.getAnnotationSpanStart(), ta.getAnnotationSpanEnd());
					}
				} else {
					ta.addSpan(new Span(spanStart, spanEnd));
					coveredText += (" " + documentText.substring(spanStart, spanEnd));
				}
			}
			ta.setCoveredText(coveredText);
			annotIdToAnnotMap.put(annotId, ta);
		}
	}

}
