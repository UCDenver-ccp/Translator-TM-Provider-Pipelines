package edu.cuanschutz.ccp.tm_provider.relation_extraction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.codec.digest.DigestUtils;

import com.google.common.annotations.VisibleForTesting;

import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.BiolinkAssociation;
import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.BiolinkClass;
import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.BiolinkPredicate;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.reader.Line;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import edu.ucdenver.ccp.common.string.StringUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentReader;
import edu.ucdenver.ccp.nlp.core.annotation.Annotator;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.impl.DefaultTextAnnotation;
import edu.ucdenver.ccp.nlp.core.mention.ClassMention;
import edu.ucdenver.ccp.nlp.core.mention.ComplexSlotMention;
import edu.ucdenver.ccp.nlp.core.mention.impl.DefaultClassMention;
import lombok.Data;

/**
 * This script transforms a set of BRAT files annotated with Biolink
 * associations into a form suitable for training a BERT model whereby the
 * entities are replaced with placeholders.
 * 
 */
/**
 * @param sentences
 * @param entityAnnots
 * @param writer
 * @throws IOException
 */
/**
 * @param sentences
 * @param entityAnnots
 * @param writer
 * @throws IOException
 */
public class BratToBertConverter {

	private static final String BRAT_ANN_FILE_SUFFIX = "ann";
	private static final CharacterEncoding UTF8 = CharacterEncoding.UTF_8;

	public enum Recurse {
		YES(true), NO(false);

		private final boolean recurse;

		private Recurse(boolean recurse) {
			this.recurse = recurse;
		}

		public boolean getValue() {
			return recurse;
		}
	}

	public static void convertBratToBert(BiolinkAssociation biolinkAssociation, File bratDirectory, Recurse recurse,
			File bertOutputFile) throws FileNotFoundException, IOException {

		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(bertOutputFile)) {
			for (Iterator<File> fileIterator = FileUtil.getFileIterator(bratDirectory, recurse.getValue(),
					BRAT_ANN_FILE_SUFFIX); fileIterator.hasNext();) {
				File annFile = fileIterator.next();
				File txtFile = getCorrespondingTxtFile(annFile);

				BioNLPDocumentReader reader = new BioNLPDocumentReader();
				TextDocument td = reader.readDocument("unknown", "unknown", annFile, txtFile, UTF8);

				List<TextAnnotation> sentences = getSentenceAnnotationsOnePerLine(txtFile);

				generateBertStyleTrainingData(biolinkAssociation, sentences, td.getAnnotations(), writer);

			}

		}
	}

	@VisibleForTesting
	protected static List<TextAnnotation> getSentenceAnnotationsOnePerLine(File txtFile) throws IOException {
		List<TextAnnotation> sentences = new ArrayList<TextAnnotation>();
		// we assume there is one sentence per line in the .txt file
		for (StreamLineIterator lineIter = new StreamLineIterator(txtFile, UTF8); lineIter.hasNext();) {
			Line line = lineIter.next();
			int sentenceStartOffset = (int) line.getCharacterOffset();
			TextAnnotation sentence = createSentenceAnnot(sentenceStartOffset,
					sentenceStartOffset + line.getText().length(), line.getText());
			sentences.add(sentence);
		}
		return sentences;
	}

	/**
	 * 
	 * @param annFile
	 * @return the corresponding .txt file for this .ann file. The file is assumed
	 *         to be named identically except for the file suffix.
	 */
	@VisibleForTesting
	protected static File getCorrespondingTxtFile(File annFile) {
		File file = new File(StringUtil.replaceSuffix(annFile.getAbsolutePath(), ".ann", ".txt"));
		if (!file.exists()) {
			throw new IllegalArgumentException(
					"Missing corresponding text file. This file does not exist: " + file.getAbsolutePath());
		}
		return file;
	}

	public static void generateBertStyleTrainingData(BiolinkAssociation biolinkAssociation,
			List<TextAnnotation> sentences, List<TextAnnotation> entityAnnots, BufferedWriter writer)
			throws IOException {
		Set<String> alreadyPrinted = new HashSet<String>();
		Map<TextAnnotation, Set<TextAnnotation>> sentToEntityMap = new HashMap<TextAnnotation, Set<TextAnnotation>>();

		addIdsToAnnotations(entityAnnots, "entity");
		entityAnnots = normalizeEntityTypes(biolinkAssociation, entityAnnots);

		populateSentenceToEntityMap(sentences, entityAnnots, sentToEntityMap);

		for (Entry<TextAnnotation, Set<TextAnnotation>> entry : sentToEntityMap.entrySet()) {
			TextAnnotation sentence = entry.getKey();
			Map<BiolinkClass, Set<TextAnnotation>> biolinkClassToEntityAnnotsMap = getEntityAnnots(biolinkAssociation,
					entry.getValue());

			/* we are interested in sentences that have >1 genes */
			if (sentenceHasRequisiteEntities(biolinkAssociation, biolinkClassToEntityAnnotsMap)) {
				Set<Assertion> assertions = getAssertions(biolinkAssociation, biolinkClassToEntityAnnotsMap);

				for (Assertion assertion : assertions) {
					writeTrainingExample(writer, sentence, assertion, alreadyPrinted);
				}

			}
		}
	}

	/**
	 * After manual annotation there may be corrections applied to the original
	 * concept annotations, e.g. there may be mentions of correct_chemical or
	 * missing_chemical. If corrected_chemical overlaps with the original chemical
	 * (in the case where the human annotator did not remove the original chemical
	 * annotaiton) we want to remove the original chemical annotation. Once all
	 * original annotations have been removed, as appropriate, then we normalize the
	 * types, e.g. convert corrected_chemical to chemical.
	 * 
	 * @param annots
	 */
	@VisibleForTesting
	protected static List<TextAnnotation> normalizeEntityTypes(BiolinkAssociation biolinkAssociation,
			List<TextAnnotation> annots) {
		Set<TextAnnotation> toRemove = new HashSet<TextAnnotation>();
		for (TextAnnotation annot1 : annots) {
			for (TextAnnotation annot2 : annots) {
				if (!annot1.equals(annot2)) {
					if (annot1.overlaps(annot2)) {
						String annot1Type = annot1.getClassMention().getMentionName();
						String annot2Type = annot2.getClassMention().getMentionName();

						if (annot1Type.contains("corrected")) {
							toRemove.add(annot2);
						} else if (annot2Type.contains("corrected")) {
							toRemove.add(annot1);
						}
					}
				}
			}
		}

		List<TextAnnotation> toReturn = new ArrayList<TextAnnotation>();
		for (TextAnnotation annot : annots) {
			if (!toRemove.contains(annot)) {
				toReturn.add(annot);

				String annotType = annot.getClassMention().getMentionName();
				if (annotType.startsWith("corrected_")) {
					annotType = StringUtil.removePrefix(annotType, "corrected_");
					annot.getClassMention().setMentionName(annotType);
				} else if (annotType.startsWith("missed_")) {
					annotType = StringUtil.removePrefix(annotType, "missed_");
					annot.getClassMention().setMentionName(annotType);
				}

			}
		}

		// convert disease/phenotype to DISEASE_OR_PHENOTYPIC_FEATURE if that is what is
		// used in the Biolink Association

		for (TextAnnotation annot : toReturn) {
			String type = annot.getClassMention().getMentionName();
			if (biolinkAssociation.getSubjectClass() == BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE
					|| biolinkAssociation.getObjectClass() == BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE) {
				if (type.toLowerCase().equals("disease") || type.toLowerCase().equals("phenotype")
						|| type.toLowerCase().equals("phenotypic_feature")) {
					type = BiolinkClass.DISEASE_OR_PHENOTYPIC_FEATURE.name().toLowerCase();
					annot.getClassMention().setMentionName(type);
				}
			}
		}

		return toReturn;

	}

	private static void addIdsToAnnotations(List<TextAnnotation> annots, String idPrefix) {
		int index = 0;
		for (TextAnnotation annot : annots) {
			annot.setAnnotationID(idPrefix + index++);
		}
	}

	@VisibleForTesting
	protected static Set<Assertion> getAssertions(BiolinkAssociation biolinkAssociation,
			Map<BiolinkClass, Set<TextAnnotation>> biolinkClassToEntityAnnotsMap) {

		Set<Assertion> assertions = new HashSet<Assertion>();

		Set<TextAnnotation> subjectAnnots = null;
		Set<TextAnnotation> objectAnnots = null;

		if (biolinkClassToEntityAnnotsMap.size() == 1) {
			subjectAnnots = objectAnnots = biolinkClassToEntityAnnotsMap.entrySet().iterator().next().getValue();
		} else {
			for (Entry<BiolinkClass, Set<TextAnnotation>> entry : biolinkClassToEntityAnnotsMap.entrySet()) {
				if (entry.getKey() == biolinkAssociation.getSubjectClass()) {
					subjectAnnots = entry.getValue();
				} else {
					objectAnnots = entry.getValue();
				}
			}
		}

		assertions.addAll(createAllAssertions(biolinkAssociation, subjectAnnots, objectAnnots));

		return assertions;

	}

	protected static Set<? extends Assertion> createAllAssertions(BiolinkAssociation biolinkAssociation,
			Set<TextAnnotation> subjectAnnots, Set<TextAnnotation> objectAnnots) {

		Set<Assertion> assertions = new HashSet<Assertion>();

		for (TextAnnotation subjectAnnot : subjectAnnots) {
			for (TextAnnotation objectAnnot : objectAnnots) {
				if (!subjectAnnot.equals(objectAnnot)) {
					boolean hasRelation = false;
					Collection<ComplexSlotMention> complexSlotMentions = subjectAnnot.getClassMention()
							.getComplexSlotMentions();
					if (complexSlotMentions != null && complexSlotMentions.size() > 0) {
						for (ComplexSlotMention csm : complexSlotMentions) {
							for (ClassMention cm : csm.getSlotValues()) {
								TextAnnotation linkedAnnot = cm.getTextAnnotation();
								if (linkedAnnot.equals(objectAnnot)) {
									String relation = csm.getMentionName();
									Assertion assertion = new Assertion(biolinkAssociation.getSubjectClass(),
											subjectAnnot, biolinkAssociation.getObjectClass(), objectAnnot, relation);
									assertions.add(assertion);
									hasRelation = true;
									break;
								}
							}
						}
					}

					if (!hasRelation) {
						Assertion assertion = new Assertion(biolinkAssociation.getSubjectClass(), subjectAnnot,
								biolinkAssociation.getObjectClass(), objectAnnot,
								BiolinkPredicate.NO_RELATION_PRESENT.getEdgeLabelAbbreviation());
						assertions.add(assertion);
					}
				}
			}
		}

		return assertions;
	}

	@Data
	public static class Assertion {
		private final BiolinkClass subjectClass;
		private final TextAnnotation subjectAnnotation;
		private final BiolinkClass objectClass;
		private final TextAnnotation objectAnnotation;
		private final String relation;
	}

	/**
	 * @param biolinkAssociation
	 * @param biolinkClassToEntityAnnotsMap
	 * @return true if the sentence has the requisite number of entities, i.e. at
	 *         least one of each type if two types are required, or at least 2 of
	 *         one type if only one type is required.
	 */
	private static boolean sentenceHasRequisiteEntities(BiolinkAssociation biolinkAssociation,
			Map<BiolinkClass, Set<TextAnnotation>> biolinkClassToEntityAnnotsMap) {
		if (biolinkAssociation.getSubjectClass() == biolinkAssociation.getObjectClass()
				&& biolinkClassToEntityAnnotsMap.containsKey(biolinkAssociation.getSubjectClass())) {
			return biolinkClassToEntityAnnotsMap.get(biolinkAssociation.getSubjectClass()).size() > 1;
		} else {
			return biolinkClassToEntityAnnotsMap.containsKey(biolinkAssociation.getSubjectClass())
					&& biolinkClassToEntityAnnotsMap.containsKey(biolinkAssociation.getObjectClass());
		}
	}

	private static void writeTrainingExample(BufferedWriter writer, TextAnnotation sentence, Assertion assertion,
			Set<String> alreadyPrinted) throws IOException {
		String out = getTrainingExampleLine(sentence, assertion, alreadyPrinted);
		if (writer != null && out != null) {
			writer.write(out + "\n");
		}
	}

	/**
	 * Populates the sentToEntityMap by looking for entity annotations that overlap
	 * with sentence annotations
	 * 
	 * @param sentences
	 * @param entityAnnots
	 * @param sentToEntityMap
	 */
	private static void populateSentenceToEntityMap(List<TextAnnotation> sentences, List<TextAnnotation> entityAnnots,
			Map<TextAnnotation, Set<TextAnnotation>> sentToEntityMap) {
		for (TextAnnotation entityAnnot : entityAnnots) {
			for (TextAnnotation sentence : sentences) {
				if (entityAnnot.overlaps(sentence)) {
					CollectionsUtil.addToOne2ManyUniqueMap(sentence, entityAnnot, sentToEntityMap);
					/*
					 * an entity should only overlap with a single sentence, so once it's found we
					 * can break out of the sentence loop
					 */
					break;
				}
			}
		}
	}

	@VisibleForTesting
	protected static String getTrainingExampleLine(TextAnnotation sentence, Assertion assertion,
			Set<String> alreadyPrinted) {

		/*
		 * setting annotation IDs so that the proper placeholder can be used to replace
		 * the genes in the sentence
		 */

		TextAnnotation subjectAnnot = assertion.getSubjectAnnotation();
		TextAnnotation objectAnnot = assertion.getObjectAnnotation();

		Map<String, String> annotationIdToPlaceholderMap = new HashMap<String, String>();
		annotationIdToPlaceholderMap.put(subjectAnnot.getAnnotationID(), assertion.getSubjectClass().getPlaceholder());
		annotationIdToPlaceholderMap.put(objectAnnot.getAnnotationID(), assertion.getObjectClass().getPlaceholder());

		// make sure both genes are in the same sentence; don't allow the genes to
		// overlap either
		if (sentence.overlaps(subjectAnnot) && sentence.overlaps(objectAnnot) && !subjectAnnot.overlaps(objectAnnot)) {

			String sentenceText = sentence.getCoveredText();
			List<TextAnnotation> sortedEntityAnnots = Arrays.asList(subjectAnnot, objectAnnot);
			Collections.sort(sortedEntityAnnots, TextAnnotation.BY_SPAN());

			// must replace in decreasing span order
			for (int i = 1; i >= 0; i--) {
				TextAnnotation entityAnnot = sortedEntityAnnots.get(i);
				int entityStart = entityAnnot.getAggregateSpan().getSpanStart() - sentence.getAnnotationSpanStart();
				int entityEnd = entityAnnot.getAggregateSpan().getSpanEnd() - sentence.getAnnotationSpanStart();

				String placeholder = annotationIdToPlaceholderMap.get(entityAnnot.getAnnotationID());

				sentenceText = sentenceText.substring(0, entityStart) + placeholder + sentenceText.substring(entityEnd);
			}

			String id = DigestUtils.shaHex(sentenceText);
			if (!alreadyPrinted.contains(id)) {
				alreadyPrinted.add(id);
				String output = id + "\t" + sentenceText + "\t" + assertion.getRelation();
				return output;
			}
		}
		return null;

	}

//	private static Map<String, TextAnnotation> getGeneIdToGeneMap(Set<TextAnnotation> geneAnnots) {
//		Map<String, TextAnnotation> geneIdToGeneMap = new HashMap<String, TextAnnotation>();
//
//		for (TextAnnotation ta : geneAnnots) {
//			geneIdToGeneMap.put(ta.getAnnotationID(), ta);
//		}
//
//		return geneIdToGeneMap;
//	}
//
//	private static Set<String> getGeneIdPairs(Set<TextAnnotation> geneAnnots) {
//		Set<String> pairs = new HashSet<String>();
//
//		for (TextAnnotation ta1 : geneAnnots) {
//			for (TextAnnotation ta2 : geneAnnots) {
//				String id1 = ta1.getAnnotationID();
//				String id2 = ta2.getAnnotationID();
//				if (!id1.equals(id2)) {
//					pairs.add(getOrderedString(id1, id2));
//				}
//			}
//		}
//
//		return pairs;
//	}
//
//	private static String getOrderedString(String id1, String id2) {
//		List<String> list = Arrays.asList(id1, id2);
//		Collections.sort(list);
//		return list.get(0) + "_" + list.get(1);
//	}

	/**
	 * @param biolinkAssociation
	 * @param entityAnnots
	 * @return a mapping from BiolinkClass to the annotations referencing that
	 *         BiolinkClass
	 */
	@VisibleForTesting
	protected static Map<BiolinkClass, Set<TextAnnotation>> getEntityAnnots(BiolinkAssociation biolinkAssociation,
			Set<TextAnnotation> entityAnnots) {
		Map<BiolinkClass, Set<TextAnnotation>> map = new HashMap<BiolinkClass, Set<TextAnnotation>>();
		for (TextAnnotation ta : entityAnnots) {
			String type = ta.getClassMention().getMentionName();
			if (type.contains(":")) {
				String ontologyPrefix = type.substring(0, type.indexOf(":"));
				if (biolinkAssociation.getSubjectClass().getOntologyPrefixes().contains(ontologyPrefix)) {
					CollectionsUtil.addToOne2ManyUniqueMap(biolinkAssociation.getSubjectClass(), ta, map);
				} else if (biolinkAssociation.getObjectClass().getOntologyPrefixes().contains(ontologyPrefix)) {
					CollectionsUtil.addToOne2ManyUniqueMap(biolinkAssociation.getObjectClass(), ta, map);
				}
			} else {
				// we assume that the type is the biolink category, e.g. disease, chemical and
				// we map to the BiolinkClass enum
				BiolinkClass biolinkClass = BiolinkClass.valueOf(type.toUpperCase());
				CollectionsUtil.addToOne2ManyUniqueMap(biolinkClass, ta, map);
			}
		}
		return map;
	}

//	public static List<TextAnnotation> getAnnotations(String sourceId, InputStream txtStream, InputStream a12Stream)
//			throws IOException {
//		BioNLPDocumentReader reader = new BioNLPDocumentReader();
//		TextDocument td = reader.readDocument(sourceId, "PubMed", a12Stream, txtStream, CharacterEncoding.UTF_8);
//		return td.getAnnotations();
//	}

//	public static List<TextAnnotation> getSentences(InputStream txtStream) throws IOException {
//		String docText = IOUtils.toString(txtStream, CharacterEncoding.UTF_8.getCharacterSetName());
//		InputStream modelStream = ClassPathUtil.getResourceStreamFromClasspath(GeneRegCorpusReader.class,
//				"/de/tudarmstadt/ukp/dkpro/core/opennlp/lib/sentence-en-maxent.bin");
//		SentenceModel model = new SentenceModel(modelStream);
//		SentenceDetectorME sentenceDetector = new SentenceDetectorME(model);
//
//		List<TextAnnotation> annots = new ArrayList<TextAnnotation>();
//		Span[] spans = sentenceDetector.sentPosDetect(docText);
//		for (Span span : spans) {
//			span.getStart();
//			span.getEnd();
//			span.getType();
//			TextAnnotation annot = createSentenceAnnot(span.getStart(), span.getEnd(),
//					span.getCoveredText(docText).toString());
//			annots.add(annot);
//		}
//
//		List<TextAnnotation> toKeep = splitSentencesOnLineBreaks(annots);
//
//		return toKeep;
//	}

//	private static List<TextAnnotation> splitSentencesOnLineBreaks(List<TextAnnotation> annots) {
//		/*
//		 * divide any sentences with line breaks into multiple sentences, splitting at
//		 * the line breaks
//		 */
//		List<TextAnnotation> toKeep = new ArrayList<TextAnnotation>();
//		for (TextAnnotation annot : annots) {
//			String coveredText = annot.getCoveredText();
//			if (coveredText.contains("\n")) {
//				String[] sentences = coveredText.split("\\n");
//				int index = annot.getAnnotationSpanStart();
//				for (String s : sentences) {
//					if (!s.isEmpty()) {
//						TextAnnotation sentAnnot = createSentenceAnnot(index, index + s.length(), s);
//						index = index + s.length() + 1;
//						toKeep.add(sentAnnot);
//					} else {
//						index++;
//					}
//				}
//				// validate - span end of more recently added sentence should be equal to the
//				// span end of the original annot
//				int originalSpanEnd = annot.getAnnotationSpanEnd();
//				int end = toKeep.get(toKeep.size() - 1).getAnnotationSpanEnd();
//				assert end == originalSpanEnd;
//			} else {
//				toKeep.add(annot);
//			}
//		}
//		return toKeep;
//	}

	private static TextAnnotation createSentenceAnnot(int spanStart, int spanEnd, String coveredText) {
		DefaultTextAnnotation annot = new DefaultTextAnnotation(spanStart, spanEnd);
		annot.setCoveredText(coveredText);
		DefaultClassMention cm = new DefaultClassMention("sentence");
		annot.setClassMention(cm);
		annot.setAnnotator(new Annotator(null, "OpenNLP", "OpenNLP"));
		return annot;
	}

}
