package edu.cuanschutz.ccp.tm_provider.corpora.craft;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileReaderUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentReader;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentWriter;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;

/**
 * This class was originally designed to aggregate the CRAFT concept annotations
 * and remove any nested concepts, e.g., if there is a CL annotation for "red
 * blood cell" and an overlapping UBERON annotation for "blood", we will exclude
 * the UBERON annotation.
 *
 */
public class ExcludeCraftNestedConcepts {

	// We aren't using MOP currently in Translator, so we will exclude for now;
	// there are too few MOP annotations
	public enum Ont {
		CHEBI, CL, GO_BP, GO_CC, GO_MF, MONDO, // MOP,
		NCBITaxon, PR, SO, UBERON, DRUGBANK, HP, SNOMEDCT
	}

	public enum WithExtensionClasses {
		YES, NO
	}

	private static final CharacterEncoding ENCODING = CharacterEncoding.UTF_8;

	/**
	 * Cycle through each document in CRAFT, aggregate existing concept annotations
	 * to determine which are nested and should be excluded, log all exclusions,
	 * serialize new versions of the concept annotation files where excluded concept
	 * annotations have been removed.
	 * 
	 * @param craftBaseDir
	 * @param outputBionlpBaseDir
	 * @param inputBionlpBaseDir
	 * @param excludeOverlaps
	 * @param docText
	 * @throws IOException
	 */
	public static void excludeNestedAnnotations(File craftBaseDir, File inputBionlpBaseDir, File outputBionlpBaseDir,
			ExcludeExactOverlaps excludeOverlaps) throws IOException {
		List<String> docIds = loadCraftDocumentIds(craftBaseDir);

		File exclusionLogDir = new File(outputBionlpBaseDir.getParentFile(), "nested-exclusions");
		if (!exclusionLogDir.exists()) {
			exclusionLogDir.mkdirs();
		}

		for (String docId : docIds) {
			File logFile = new File(exclusionLogDir, String.format("%s.exclusion_log", docId));
			try (BufferedWriter logWriter = FileWriterUtil.initBufferedWriter(logFile)) {
//				for (WithExtensionClasses ext : WithExtensionClasses.values()) {
				WithExtensionClasses ext = WithExtensionClasses.NO;
				Map<Ont, TextDocument> ontToDocMap = loadOntToDocMap(docId, craftBaseDir, inputBionlpBaseDir, ext);

				filterNestedConceptAnnotations(ontToDocMap, ontToDocMap.values().iterator().next().getText(), logWriter,
						excludeOverlaps);
				serializeAnnotationFiles(docId, ontToDocMap, outputBionlpBaseDir, ext);
//				}
			}
		}

	}

	/**
	 * Update the input map by removing nested annotations from each of the
	 * individual TextDocuments
	 * 
	 * @param ontToDocMap
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	protected static void filterNestedConceptAnnotations(Map<Ont, TextDocument> ontToDocMap, String docText,
			BufferedWriter logWriter, ExcludeExactOverlaps excludeOverlaps) throws FileNotFoundException, IOException {
		// aggregate all annotations into a single document so that we can then
		// determine which are nested
		TextDocument aggTd = new TextDocument("agg", "agg", docText);

		for (Entry<Ont, TextDocument> entry : ontToDocMap.entrySet()) {
			// add an annotation id to every annotation; to be used during the removal
			// process below
			addAnnotationIds(entry.getKey(), entry.getValue());
			aggTd.addAnnotations(entry.getValue().getAnnotations());
		}

		Set<TextAnnotation> nestedAnnotations = identifyNestedAnnotations(aggTd.getAnnotations(), logWriter,
				excludeOverlaps);
		removeAnnotations(nestedAnnotations, ontToDocMap);
		verifyNoNested(ontToDocMap, excludeOverlaps);

	}

	/**
	 * Verify that there are no nested annotations. If ExcludeExactOverlaps = NO,
	 * then we allow exact overlaps to remain.
	 * 
	 * @param ontToDocMap
	 * @param excludeOverlaps
	 */
	private static void verifyNoNested(Map<Ont, TextDocument> ontToDocMap, ExcludeExactOverlaps excludeExactOverlaps) {
		Set<TextAnnotation> allAnnots = new HashSet<TextAnnotation>();

		for (TextDocument td : ontToDocMap.values()) {
			allAnnots.addAll(td.getAnnotations());
		}

		for (TextAnnotation annot1 : allAnnots) {
			for (TextAnnotation annot2 : allAnnots) {
				if (!annot1.equals(annot2)) {
					if (annot1.overlaps(annot2)
							&& (excludeExactOverlaps == ExcludeExactOverlaps.CHOOSE_ONE_SEMI_RANDOMLY
									|| (excludeExactOverlaps == ExcludeExactOverlaps.NO
											&& !annot1.getSpans().equals(annot2.getSpans())))) {
						throw new IllegalArgumentException(String.format("Detected unexpected overlap between:\n%s\n%s",
								annot1.getSingleLineRepresentation(), annot2.getSingleLineRepresentation()));
					}
				}
			}
		}

	}

	/**
	 * @param annotations
	 * @param logWriter
	 * @return a set of annotations that were determined to be nested within another
	 *         annotation
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public static Set<TextAnnotation> identifyNestedAnnotations(List<TextAnnotation> annotations,
			BufferedWriter logWriter, ExcludeExactOverlaps excludeOverlaps) throws FileNotFoundException, IOException {
		List<Set<TextAnnotation>> overlappingAnnotSets = getOverlappingAnnotSets(annotations);
		Set<TextAnnotation> nestedAnnotations = new HashSet<TextAnnotation>();
		for (Set<TextAnnotation> overlappingSet : overlappingAnnotSets) {
			Set<TextAnnotation> nested = identifyNestedAnnotations(overlappingSet, logWriter, excludeOverlaps);
			nestedAnnotations.addAll(nested);
		}
		return nestedAnnotations;
	}

	/**
	 * group any annotations that overlap into sets
	 * 
	 * @param annotations
	 * @return
	 */
	protected static List<Set<TextAnnotation>> getOverlappingAnnotSets(List<TextAnnotation> annotations) {
		List<Set<TextAnnotation>> overlappingSets = new ArrayList<Set<TextAnnotation>>();

		Collections.sort(annotations, TextAnnotation.BY_SPAN());

		Set<TextAnnotation> overlaps = null;
		TextAnnotation prevAnnot = null;
		for (TextAnnotation annot : annotations) {

			if (prevAnnot == null) {
				// at the beginning, the previous annot is null, so there is nothing to overlap
				prevAnnot = annot;
				continue;
			}

			if (overlaps != null) {
				boolean foundOverlap = false;
				// then we have detected a recent overlap already - so we will compare the
				// current annotation to all annotations in that overlapping set
				for (TextAnnotation overlap : overlaps) {
					if (annot.overlaps(overlap)) {
						overlaps.add(annot);
						foundOverlap = true;
						break;
					}
				}

				// if we found an overlap then we will continue to the next annotation to see if
				// it also overlaps with this set -- if we did not find any overlaps, then this
				// set is complete and can be added to the overlappingSets list.
				if (!foundOverlap) {
					overlappingSets.add(new HashSet<TextAnnotation>(overlaps));
					overlaps = null;
				}
			} else {
				// then there's no known overlaps currently, so we will compare this annotation
				// to the previous annot to see if they overlap -- if they do, we start a new
				// overlaps set
				if (annot.overlaps(prevAnnot)) {
					overlaps = new HashSet<TextAnnotation>();
					overlaps.add(annot);
					overlaps.add(prevAnnot);
				}
			}
			prevAnnot = annot;
		}

		// if there are remaining overlaps then add a new set
		if (overlaps != null) {
			overlappingSets.add(new HashSet<TextAnnotation>(overlaps));
		}

		return overlappingSets;
	}

	public enum ExcludeExactOverlaps {
		CHOOSE_ONE_SEMI_RANDOMLY, NO
	}

	/**
	 * Given a set of overlapping annotations, identify those that should be
	 * declared "nested" and eventually excluded.
	 * 
	 * @param overlappingSet
	 * @return
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	protected static Set<TextAnnotation> identifyNestedAnnotations(Set<TextAnnotation> overlappingSet,
			BufferedWriter logWriter, ExcludeExactOverlaps excludeOverlaps) throws FileNotFoundException, IOException {
		// if one or more annotations are encompassed by another, e.g. "blood" is
		// encompassed by "red blood cell" then we keep the encompassing annotation
		Set<TextAnnotation> nested = new HashSet<TextAnnotation>();
		for (TextAnnotation annot1 : overlappingSet) {
			for (TextAnnotation annot2 : overlappingSet) {
				if (!annot1.equals(annot2)) {
					if (annot1.getAggregateSpan().equals(annot2.getAggregateSpan())) {

						// keep them both - if we want to consolidate, do that explicitly in a separate
						// step similar to how we consolidate MONDO/HP concepts.

//						// TODO: If the annotations are to the same span, but from two different
//						// ontologies then we pick one randomly for now; this is not ideal - but to make
//						// it deterministic we will select the one with the concept id that is
//						// alphabetically last -- this is to get PR instead of GO_CC. The ontology is
//						// the first part of the annotation ID, so we can used that as a proxy here.
						if (excludeOverlaps == ExcludeExactOverlaps.CHOOSE_ONE_SEMI_RANDOMLY) {
							Map<String, TextAnnotation> map = new HashMap<String, TextAnnotation>();
							String id1 = annot1.getClassMention().getMentionName();
							String id2 = annot2.getClassMention().getMentionName();
							map.put(id1, annot1);
							map.put(id2, annot2);
							List<String> ids = Arrays.asList(id1, id2);
							Collections.sort(ids);
							nested.add(map.get(ids.get(1)));
						}
					} else if (encompasses(annot1, annot2)) {
						nested.add(annot2);
					} else if (encompasses(annot2, annot1)) {
						nested.add(annot1);
					} else if (annot1.overlaps(annot2)) {
						// if we have overlap but not complete encompassing -- in this case we will keep
						// the concept mention that appears first
						TextAnnotation keep = null;
						TextAnnotation discard = null;

						if (annot1.getAnnotationSpanStart() < annot2.getAnnotationSpanStart()) {
							keep = annot1;
							discard = annot2;
						} else {
							keep = annot2;
							discard = annot1;
						}

						nested.add(discard);
//						System.out.println(String.format("keeping: %s [%d..%d] %s instead of: %s [%d..%d] %s",
//								keep.getCoveredText(), keep.getAnnotationSpanStart(), keep.getAnnotationSpanEnd(),
//								keep.getClassMention().getMentionName(), discard.getCoveredText(),
//								discard.getAnnotationSpanStart(), discard.getAnnotationSpanEnd(),
//								discard.getClassMention().getMentionName()));

					}
				}
			}
		}

//		// if there is overlap, but not complete encompassing, then what?
//		if (nested.size() + 1 != overlappingSet.size()) {
//			StringBuilder errorMsg = new StringBuilder();
//			for (TextAnnotation annot : nested) {
////				errorMsg.append(String.format("NESTED: %s [%d..%d] %s\n", annot.getCoveredText(),
////						annot.getAnnotationSpanStart(), annot.getAnnotationSpanEnd(), annot.getClassMention().getMentionName()));
//				errorMsg.append(String.format("NESTED: %s\n", annot.getSingleLineRepresentation()));
//			}
//			for (TextAnnotation annot : overlappingSet) {
////				errorMsg.append(String.format("OVER: %s [%d..%d] %s\n", annot.getCoveredText(),
////						annot.getAnnotationSpanStart(), annot.getAnnotationSpanEnd(), annot.getClassMention().getMentionName()));
//				errorMsg.append(String.format("OVER:   %s\n", annot.getSingleLineRepresentation()));
//			}
//
////			throw new IllegalStateException(
////					"Not sure how to handle non-encompassing overlap...\n" + errorMsg.toString());
//		}
//
//		overlappingSet.removeAll(nested);
//		if (logWriter != null) {
//			logNestedFiltering(logWriter, overlappingSet.iterator().next(), nested);
//		}

		return nested;
	}

	/**
	 * Log the annotation that will be kept, and those that were declared nested to
	 * a file
	 * 
	 * @param logWriter
	 * @param keep
	 * @param nested
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private static void logNestedFiltering(BufferedWriter logWriter, TextAnnotation keep, Set<TextAnnotation> nested)
			throws FileNotFoundException, IOException {

		logWriter.write(String.format("KEEP: %s [%d..%d] %s\n", keep.getCoveredText(), keep.getAnnotationSpanStart(),
				keep.getAnnotationSpanEnd(), keep.getClassMention().getMentionName()));
		for (TextAnnotation annot : nested) {
			logWriter.write(
					String.format("EXCL: %s [%d..%d] %s\n", annot.getCoveredText(), annot.getAnnotationSpanStart(),
							annot.getAnnotationSpanEnd(), annot.getClassMention().getMentionName()));
		}
		logWriter.write("\n");
	}

	/**
	 * @param annot1
	 * @param annot2
	 * @return true if annot2 is completely enclosed by annot1
	 */
	protected static boolean encompasses(TextAnnotation annot1, TextAnnotation annot2) {
		Span span1 = annot1.getAggregateSpan();
		Span span2 = annot2.getAggregateSpan();
		return span2.getSpanStart() >= span1.getSpanStart() && span2.getSpanEnd() <= span1.getSpanEnd();
	}

	/**
	 * Remove annotations in the toRemove set from the TextDocuments in the
	 * ontToDocMap
	 * 
	 * @param nestedAnnotations
	 * @param ontToDocMap
	 */
	protected static void removeAnnotations(Set<TextAnnotation> toRemove, Map<Ont, TextDocument> ontToDocMap) {
		for (TextAnnotation annot : toRemove) {
			Ont ont = Ont.valueOf(annot.getAnnotationID().split("\\|")[0]);
			TextDocument td = ontToDocMap.get(ont);
			if (td.getAnnotations().contains(annot)) {
				td.getAnnotations().remove(annot);
			} else {
				throw new IllegalArgumentException("Unable to remove annotation.");
			}
		}
	}

	/**
	 * add an annotation ID to each annotation that consists of the ontology + an
	 * integer
	 * 
	 * @param ont
	 * @param td
	 */
	private static void addAnnotationIds(Ont ont, TextDocument td) {
		int index = 0;
		for (TextAnnotation annot : td.getAnnotations()) {
			annot.setAnnotationID(String.format("%s|%d", ont.name(), index++));
		}
	}

	/**
	 * Write the updated documents to file, replacing the previous version
	 * 
	 * @param docId
	 * @param ontToDocMap
	 * @param craftBaseDir
	 * @param ext
	 * @throws IOException
	 */
	protected static void serializeAnnotationFiles(String docId, Map<Ont, TextDocument> ontToDocMap, File bionlpBaseDir,
			WithExtensionClasses ext) throws IOException {
		BioNLPDocumentWriter bionlpWriter = new BioNLPDocumentWriter();
		for (Entry<Ont, TextDocument> entry : ontToDocMap.entrySet()) {
			File annotFile = getAnnotFile(entry.getKey(), docId, bionlpBaseDir, ext);
			File dir = annotFile.getParentFile();
			if (!dir.exists()) {
				dir.mkdirs();
			}
			bionlpWriter.serialize(entry.getValue(), annotFile, ENCODING);
		}
	}

	/**
	 * Return a map populated with a {@link TextDocument} for each ontology for the
	 * specified document ID
	 * 
	 * @param docId
	 * @param craftBaseDir
	 * @param originalBionlpBaseDir
	 * @param ext
	 * @return
	 * @throws IOException
	 */
	protected static Map<Ont, TextDocument> loadOntToDocMap(String docId, File craftBaseDir, File originalBionlpBaseDir,
			WithExtensionClasses ext) throws IOException {
		BioNLPDocumentReader reader = new BioNLPDocumentReader();
		Map<Ont, TextDocument> map = new HashMap<Ont, TextDocument>();
		File txtFile = getTextFile(docId, craftBaseDir);
		for (Ont ont : EnumSet.of(Ont.CHEBI, Ont.CL, Ont.GO_BP, Ont.GO_CC, // Ont.GO_MF, Excluding GO_MF as it overlaps
																			// with PR too muchs
				Ont.MONDO, Ont.NCBITaxon, Ont.PR, Ont.SO, Ont.UBERON)) {
			File annotFile = getAnnotFile(ont, docId, originalBionlpBaseDir, ext);
			TextDocument td = reader.readDocument(docId, "craft", annotFile, txtFile, ENCODING);
			map.put(ont, td);
		}
		return map;
	}

	/**
	 * @param ont
	 * @param docId
	 * @param craftBaseDir
	 * @param ext
	 * @return a reference to the annotation file for the specified ontology and
	 *         document ID
	 */
	private static File getAnnotFile(Ont ont, String docId, File bionlpBaseDir, WithExtensionClasses ext) {
		File ontDir = new File(bionlpBaseDir,
				((ext == WithExtensionClasses.YES) ? String.format("%s_ext", ont.name().toLowerCase())
						: ont.name().toLowerCase()));
		File annotFile = new File(ontDir, String.format("%s.bionlp", docId));
		return annotFile;
	}

	/**
	 * @param docId
	 * @param craftBaseDir
	 * @return a reference to the text file for the specified document ID
	 */
	public static File getTextFile(String docId, File craftBaseDir) {
		File txtFile = new File(craftBaseDir, String.format("articles/txt/%s.txt", docId));
		if (txtFile.exists()) {
			return txtFile;
		}
		throw new IllegalStateException("Text file does not exist: " + txtFile.getAbsolutePath());
	}

	/**
	 * @param craftBaseDir
	 * @return a list of document IDs in the CRAFT corpus
	 * @throws IOException
	 */
	public static List<String> loadCraftDocumentIds(File craftBaseDir) throws IOException {
		File craftIdsFile = new File(craftBaseDir, "articles/ids/craft-pmids.txt");
		return FileReaderUtil.loadLinesFromFile(craftIdsFile, ENCODING);
	}

	public static void main(String[] args) {
		File craftBaseDir = new File("/Users/bill/projects/craft-shared-task/exclude-nested-concepts/craft.git");
		File inputBionlpBaseDir = new File(
				"/Users/bill/projects/craft-shared-task/exclude-nested-concepts/craft-shared-tasks.git/bionlp-exclude-specific");

		try {
			// use this for the eval pipeline b/c it includes overlaps that match spans
			// exactly
			System.out.println("Processing for bionlp-no-nested");
			ExcludeExactOverlaps excludeOverlaps = ExcludeExactOverlaps.NO;
			File outputBionlpBaseDir = new File(
					"/Users/bill/projects/craft-shared-task/exclude-nested-concepts/craft-shared-tasks.git/bionlp-no-nested");

			outputBionlpBaseDir.mkdirs();
			excludeNestedAnnotations(craftBaseDir, inputBionlpBaseDir, outputBionlpBaseDir, excludeOverlaps);
			ExcludeCraftConceptsByOntologyId.validateExcludedClasses(outputBionlpBaseDir, craftBaseDir);

			// use this for the CRF/Transformer training b/c it does not permit any overlaps
			System.out.println("Processing for bionlp-no-nested-for-crf");
			excludeOverlaps = ExcludeExactOverlaps.CHOOSE_ONE_SEMI_RANDOMLY;
			outputBionlpBaseDir = new File(
					"/Users/bill/projects/craft-shared-task/exclude-nested-concepts/craft-shared-tasks.git/bionlp-no-nested-for-crf");

			outputBionlpBaseDir.mkdirs();
			excludeNestedAnnotations(craftBaseDir, inputBionlpBaseDir, outputBionlpBaseDir, excludeOverlaps);
			ExcludeCraftConceptsByOntologyId.validateExcludedClasses(outputBionlpBaseDir, craftBaseDir);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
