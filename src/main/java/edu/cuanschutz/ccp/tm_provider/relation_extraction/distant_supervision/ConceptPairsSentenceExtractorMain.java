package edu.cuanschutz.ccp.tm_provider.relation_extraction.distant_supervision;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import edu.cuanschutz.ccp.tm_provider.etl.util.BiolinkConstants.BiolinkAssociation;
import edu.cuanschutz.ccp.tm_provider.relation_extraction.BratToBertConverter;
import edu.cuanschutz.ccp.tm_provider.relation_extraction.BratToBertConverter.Assertion;
import edu.cuanschutz.ccp.tm_provider.relation_extraction.distant_supervision.ConceptPairsFileParser.ConceptPair;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil.FileSuffixEnforcement;
import edu.ucdenver.ccp.common.file.FileWriterUtil.WriteMode;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;

public class ConceptPairsSentenceExtractorMain {

	public static void main(String[] args) {
		String elasticHostname = args[0];
		int elasticPort = Integer.parseInt(args[1]);
		String apiKey = args[2];

		File outputBaseDir = new File(
				"/Users/bill/projects/ncats-translator/relations/distant-supervision/bert-training-files");

//		BiolinkAssociation association = BiolinkAssociation.BL_DISEASE_TO_PHENOTYPIC_FEATURE;
//		File conceptPairsFile = new File(
//				"/Users/bill/projects/ncats-translator/relations/distant-supervision/concept-pair-files/disease-phenotype.tsv.gz");
////				"/Users/bill/projects/ncats-translator/relations/distant-supervision/concept-pair-files/sample-pairs-10.tsv");
//		String targetPredicateUri = ConceptPairsFileParser.RO_HAS_PHENOTYPE;

		BiolinkAssociation association = BiolinkAssociation.BL_GENE_TO_CELLULAR_COMPONENT;
		File conceptPairsFile = new File(
				"/Users/bill/projects/ncats-translator/relations/distant-supervision/concept-pair-files/protein-go_cc.pr_promoted.tsv.gz");
		String targetPredicateUri = ConceptPairsFileParser.RO_LOCATED_IN;

//		BiolinkAssociation association = BiolinkAssociation.BL_GENE_TO_CELL;
//		File conceptPairsFile = new File(
//				"/Users/bill/projects/ncats-translator/relations/distant-supervision/concept-pair-files/protein-cell.cl.pr_promotedtsv.gz");
//		String targetPredicateUri = ConceptPairsFileParser.RO_LOCATED_IN;
//		
//		
//		BiolinkAssociation association = BiolinkAssociation.BL_GENE_TO_ANATOMICAL_ENTITY;
//		File conceptPairsFile = new File(
//				"/Users/bill/projects/ncats-translator/relations/distant-supervision/concept-pair-files/protein-anatomy.pr_promoted.tsv.gz");
//		String targetPredicateUri = ConceptPairsFileParser.RO_LOCATED_IN;

		// find sentences that contain established pairs of concepts
		String indexName = "sentences";
		int maxReturned = 10;

		// this part will be useful for finding negative examples
		Set<Set<String>> ontologyPrefixes = new HashSet<Set<String>>();
		ontologyPrefixes.add(new HashSet<String>(association.getSubjectClass().getOntologyPrefixes()));
		ontologyPrefixes.add(new HashSet<String>(association.getObjectClass().getOntologyPrefixes()));

		//
//		Set<Set<String>> ontologyPrefixes = new HashSet<Set<String>>();
//		ontologyPrefixes.add(new HashSet<String>(Arrays.asList("MONDO")));
//		ontologyPrefixes.add(new HashSet<String>(Arrays.asList("HP")));

		try {

			File outputDir = new File(outputBaseDir, association.name().toLowerCase());
			outputDir.mkdirs();

			boolean doPos = true;
			boolean doNeg = false;

			// select a random sampling of concept pairs then search the index for sentences
			// with each pair of concept mentions
			if (doPos) {
				Set<ConceptPair> positiveConceptPairs = ConceptPairsFileParser.extractPositivePairs(conceptPairsFile,
						targetPredicateUri);
				System.out.println("Searching for positives...");
				String posNegLabel = "pos";
				File outputFile = new File(outputDir, association.name().toLowerCase() + "." + posNegLabel + ".bert");
				try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(outputFile, CharacterEncoding.UTF_8,
						WriteMode.OVERWRITE, FileSuffixEnforcement.OFF)) {
					ConceptPairSentenceExtractor.search(elasticHostname, elasticPort, apiKey, indexName, maxReturned,
							positiveConceptPairs, writer, association,
							ConceptPairSentenceExtractor.IDENTIFIERS_TO_EXCLUDE);
				}
			}

			if (doNeg) {
				Set<ConceptPair> negativeConceptPairs = ConceptPairsFileParser.extractNegativePairs(conceptPairsFile,
						targetPredicateUri);
				System.out.println("Searching for negatives...");
				String posNegLabel = "neg";
				File outputFile = new File(outputDir, association.name().toLowerCase() + "." + posNegLabel + ".bert");
				try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(outputFile, CharacterEncoding.UTF_8,
						WriteMode.OVERWRITE, FileSuffixEnforcement.OFF)) {
					ConceptPairSentenceExtractor.search(elasticHostname, elasticPort, apiKey, indexName, maxReturned,
							negativeConceptPairs, writer, association,
							ConceptPairSentenceExtractor.IDENTIFIERS_TO_EXCLUDE);
				}
			}

//			writeToBertFile(outputDir, association, positiveSearchResults, "pos", WriteMode.OVERWRITE);

			// find examples in the data of sentences that don't use pairs of concepts that
			// are known to associate
			// if possible weight towards more specific concepts
//			Set<TextDocument> negativeSearchResults = ElasticsearchToBratExporter.search(elasticHostname, elasticPort,
//					apiKey, indexName, maxReturned, ontologyPrefixes, null, association,
//					ElasticsearchToBratExporter.IDENTIFIERS_TO_EXCLUDE);
//			System.out.println("Negative hit count: " + negativeSearchResults.size());

//			System.out.println("Searching for negatives...");
//			Map<ConceptPair, Set<TextDocument>> negativeSearchResults = ConceptPairSentenceExtractor
//					.search(elasticHostname, elasticPort, apiKey, indexName, maxReturned, negativeConceptPairs);
//
//			System.out.println("Writing BERT input file...");
//			writeToBertFile(outputDir, association, negativeSearchResults, "neg", WriteMode.OVERWRITE);

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

//	private static Set<String> createPositivePairKeys(Set<ConceptPair> conceptPairs) {
//		Set<String> keys = new HashSet<String>();
//
//		for (ConceptPair cp : conceptPairs) {
//			keys.add(createPairKey(cp.getSubjectCurie(), cp.getObjectCuries()));
//		}
//
//		return keys;
//	}

	private static void writeToBertFile(File outputDirectory, BiolinkAssociation association,
			Map<ConceptPair, Set<TextDocument>> searchResults, String posNegLabel, WriteMode writeMode,
			Set<String> conceptIdsToExclude) throws IOException {

		File outputFile = new File(outputDirectory, association.name().toLowerCase() + "." + posNegLabel + ".bert");
		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(outputFile, CharacterEncoding.UTF_8, writeMode,
				FileSuffixEnforcement.OFF)) {
			writeMaskedSentences(association, searchResults, writer, conceptIdsToExclude);

		}

	}

//	private static void writeToBertFile(BufferedWriter writer, File outputDirectory, BiolinkAssociation association,
//			ConceptPair cp, Set<TextDocument> sentences, String posNegLabel, WriteMode writeMode)
//			throws IOException {
//
//		
//			writeMaskedSentences(association, sentences, writer);
//
//	}

	private static void writeMaskedSentences(BiolinkAssociation association,
			Map<ConceptPair, Set<TextDocument>> searchResults, BufferedWriter writer, Set<String> conceptIdsToExclude)
			throws IOException {
		for (Entry<ConceptPair, Set<TextDocument>> entry : searchResults.entrySet()) {
			ConceptPair cp = entry.getKey();
			for (TextDocument td : entry.getValue()) {
				List<String> bertTrainingLines = getBertTrainingLines(td, cp, association, conceptIdsToExclude);
				for (String trainingLine : bertTrainingLines) {
					writer.write(trainingLine + "\n");
				}
			}
		}
	}

	protected static void writeMaskedSentences(BiolinkAssociation association, ConceptPair cp,
			Set<TextDocument> sentences, BufferedWriter writer, Set<String> conceptIdsToExclude) throws IOException {
		for (TextDocument td : sentences) {
			List<String> bertTrainingLines = getBertTrainingLines(td, cp, association, conceptIdsToExclude);
			for (String trainingLine : bertTrainingLines) {
				writer.write(trainingLine + "\n");
			}
		}
	}

//	private static Set<String> getNegativeMaskedSentences(TextDocument td, BiolinkAssociation association,
//			Set<String> positivePairKeys, Set<String> alreadyPrinted) {
//		Map<String, Set<TextAnnotation>> curieToAnnotMap = new HashMap<String, Set<TextAnnotation>>();
//		curieToAnnotMap.put(association.getSubjectClass().name(), new HashSet<TextAnnotation>());
//		curieToAnnotMap.put(association.getObjectClass().name(), new HashSet<TextAnnotation>());
//
//		Map<String, String> ontPrefixToBiolinkClassName = new HashMap<String, String>();
//		for (String ontPrefix : association.getSubjectClass().getOntologyPrefixes()) {
//			ontPrefixToBiolinkClassName.put(ontPrefix, association.getSubjectClass().name());
//		}
//		for (String ontPrefix : association.getObjectClass().getOntologyPrefixes()) {
//			ontPrefixToBiolinkClassName.put(ontPrefix, association.getObjectClass().name());
//		}
//
//		// classify each annotation as either the subject curie, the object curie, or
//		// not (and not overlapping the subject or object annotation -- if there is
//		// overlap, then that annotation gets discarded), and if not then clasify as the
//		// subject class, or the object class --
//		for (TextAnnotation annot : td.getAnnotations()) {
//			String conceptId = annot.getClassMention().getMentionName();
//			// it's not the subject or object curie, so place it in the appropriate bin
//			// according to ontology prefix
//			String ontPrefix = conceptId.split(":")[0];
//			String biolinkClassName = ontPrefixToBiolinkClassName.get(ontPrefix);
//			CollectionsUtil.addToOne2ManyUniqueMap(biolinkClassName, annot, curieToAnnotMap);
//		}
//
//		// debugging
//		for (Entry<String, Set<TextAnnotation>> entry : curieToAnnotMap.entrySet()) {
//			for (TextAnnotation annot : entry.getValue()) {
//				System.out.println(entry.getKey() + " -- " + annot.getClassMention().getMentionName() + " | "
//						+ annot.getCoveredText());
//			}
//		}
//
//		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();
//		TextAnnotation sentenceAnnot = factory.createAnnotation(0, td.getText().length(), td.getText(), "sentence");
//
//		Set<String> maskedSentences = new HashSet<String>();
//
//		Set<TextAnnotation> subjectAnnots = curieToAnnotMap.get(association.getSubjectClass().name());
//		Set<TextAnnotation> objectAnnots = curieToAnnotMap.get(association.getObjectClass().name());
//
//		for (TextAnnotation subjectAnnot : subjectAnnots) {
//			for (TextAnnotation objectAnnot : objectAnnots) {
//				String subjCurie = subjectAnnot.getClassMention().getMentionName();
//				String objCurie = objectAnnot.getClassMention().getMentionName();
//				String key = createPairKey(subjCurie, objCurie);
//				if (!positivePairKeys.contains(key)) {
//					Assertion assertion = new Assertion(association.getSubjectClass(), subjectAnnot,
//							association.getObjectClass(), objectAnnot, "false");
//					String maskedSentence = BratToBertConverter.getTrainingExampleLine(sentenceAnnot, assertion,
//							alreadyPrinted);
//					if (!alreadyPrinted.contains(maskedSentence)) {
//						alreadyPrinted.add(maskedSentence);
//						maskedSentences.add(maskedSentence);
//					}
//				}
//			}
//		}
//
//		return maskedSentences;
//
//	}
//
//	private static String createPairKey(String subjCurie, String objCurie) {
//		List<String> ids = Arrays.asList(subjCurie, objCurie);
//		Collections.sort(ids);
//		String key = ids.toString();
//		return key;
//	}

	public enum AnnotationType {
		SUBJECT_ANNOTATION, OBJECT_ANNOTATION,
		/**
		 * An annotation with a curie that is the same type as the subject annotation
		 * curie, but is not the subject annotation curie. For example, if the curie for
		 * the subject annotation is a MONDO, then the curie for an annotation in this
		 * group would be another MONDO
		 */
		SUBJECT_TYPE_ANNOTATION,
		/**
		 * An annotation with a curie that is the same type as the object annotation
		 * curie, but is not the object annotation curie. For example, if the curie for
		 * the object annotation is a MONDO, then the curie for an annotation in this
		 * group would be another MONDO
		 */
		OBJECT_TYPE_ANNOTATION

	}

	/**
	 * @param td
	 * @param cp
	 * @param association
	 * @return both positive and negative masked examples
	 */
	@VisibleForTesting
	protected static List<String> getBertTrainingLines(TextDocument td, ConceptPair cp, BiolinkAssociation association,
			Set<String> conceptIdsToExclude) {

		Map<AnnotationType, Set<TextAnnotation>> typeToAnnotMap = new HashMap<AnnotationType, Set<TextAnnotation>>();
		typeToAnnotMap.put(AnnotationType.SUBJECT_ANNOTATION, new HashSet<TextAnnotation>());
		typeToAnnotMap.put(AnnotationType.OBJECT_ANNOTATION, new HashSet<TextAnnotation>());
		typeToAnnotMap.put(AnnotationType.SUBJECT_TYPE_ANNOTATION, new HashSet<TextAnnotation>());
		typeToAnnotMap.put(AnnotationType.OBJECT_TYPE_ANNOTATION, new HashSet<TextAnnotation>());

		Map<String, AnnotationType> ontPrefixToAnnotationTypeMap = new HashMap<String, AnnotationType>();
		for (String ontPrefix : association.getSubjectClass().getOntologyPrefixes()) {
			ontPrefixToAnnotationTypeMap.put(ontPrefix, AnnotationType.SUBJECT_TYPE_ANNOTATION);
		}
		for (String ontPrefix : association.getObjectClass().getOntologyPrefixes()) {
			ontPrefixToAnnotationTypeMap.put(ontPrefix, AnnotationType.OBJECT_TYPE_ANNOTATION);
		}

		// classify each annotation as either the subject curie, the object curie, or
		// not (and not overlapping the subject or object annotation -- if there is
		// overlap, then that annotation gets discarded), and if not then clasify as the
		// subject class, or the object class --
		for (TextAnnotation annot : td.getAnnotations()) {
			String conceptId = annot.getClassMention().getMentionName();
			if (!conceptIdsToExclude.contains(conceptId)) {
				if (conceptId.equals(cp.getSubjectCurie())) {
					CollectionsUtil.addToOne2ManyUniqueMap(AnnotationType.SUBJECT_ANNOTATION, annot, typeToAnnotMap);
				} else if (cp.getObjectCuries().contains(conceptId)) {
					CollectionsUtil.addToOne2ManyUniqueMap(AnnotationType.OBJECT_ANNOTATION, annot, typeToAnnotMap);
				} else {
					// it's not the subject or object curie, so place it in the appropriate bin
					// according to ontology prefix
					String ontPrefix = conceptId.split(":")[0];
					AnnotationType annotationType = ontPrefixToAnnotationTypeMap.get(ontPrefix);
					CollectionsUtil.addToOne2ManyUniqueMap(annotationType, annot, typeToAnnotMap);
				}
			}
		}

		// remove any annotations that are not the subject and object curie but that
		// overlap with those annotations
		Set<TextAnnotation> subObjAnnots = new HashSet<TextAnnotation>();
		subObjAnnots.addAll(typeToAnnotMap.get(AnnotationType.SUBJECT_ANNOTATION));
		subObjAnnots.addAll(typeToAnnotMap.get(AnnotationType.OBJECT_ANNOTATION));

		Set<TextAnnotation> toRemove = new HashSet<TextAnnotation>();
		for (TextAnnotation otherAnnot : typeToAnnotMap.get(AnnotationType.SUBJECT_TYPE_ANNOTATION)) {
			for (TextAnnotation subObjAnnot : subObjAnnots) {
				if (otherAnnot.overlaps(subObjAnnot)) {
					toRemove.add(otherAnnot);
					break;
				}
			}
		}
		typeToAnnotMap.get(AnnotationType.SUBJECT_TYPE_ANNOTATION).removeAll(toRemove);

		toRemove = new HashSet<TextAnnotation>();
		for (TextAnnotation otherAnnot : typeToAnnotMap.get(AnnotationType.OBJECT_TYPE_ANNOTATION)) {
			for (TextAnnotation subObjAnnot : subObjAnnots) {
				if (otherAnnot.overlaps(subObjAnnot)) {
					toRemove.add(otherAnnot);
					break;
				}
			}
		}
		typeToAnnotMap.get(AnnotationType.OBJECT_TYPE_ANNOTATION).removeAll(toRemove);

		// remove any subject annotation that overlaps with an object annotation
		// for example, in the data MONDO_retinitis_pigmentosa is linked to
		// HP_retinitis_pigmentosa
		toRemove = new HashSet<TextAnnotation>();
		for (TextAnnotation objAnnot : typeToAnnotMap.get(AnnotationType.OBJECT_TYPE_ANNOTATION)) {
			for (TextAnnotation subAnnot : typeToAnnotMap.get(AnnotationType.SUBJECT_TYPE_ANNOTATION)) {
				if (objAnnot.overlaps(subAnnot)) {
					toRemove.add(subAnnot);
					break;
				}
			}
		}
		typeToAnnotMap.get(AnnotationType.SUBJECT_TYPE_ANNOTATION).removeAll(toRemove);

		toRemove = new HashSet<TextAnnotation>();
		for (TextAnnotation objAnnot : typeToAnnotMap.get(AnnotationType.OBJECT_ANNOTATION)) {
			for (TextAnnotation subAnnot : typeToAnnotMap.get(AnnotationType.SUBJECT_ANNOTATION)) {
				if (objAnnot.overlaps(subAnnot)) {
					toRemove.add(subAnnot);
					break;
				}
			}
		}
		typeToAnnotMap.get(AnnotationType.SUBJECT_ANNOTATION).removeAll(toRemove);

		// for each pair of subject/object curie, create a masked sentence
		Set<String> alreadyPrinted = new HashSet<String>();
//		Map<String, Set<String>> labelToMaskedSentences = new HashMap<String, Set<String>>();

		Set<String> positiveBertTrainingLines = createPositiveBertTrainingLines(td.getText(), association, cp,
				typeToAnnotMap, alreadyPrinted);
		Set<String> negativeBertTrainingLines = null;
		if (!cp.getPredicateBiolink().equals("false")) {
			// if the input is a negative, then we won't look at other pairs in the sentence
			// to create more negatives as I think we can't be entirely sure they will
			// indeed be negatives.
			negativeBertTrainingLines = createNegativeTrainingLines(td.getText(), association, cp, typeToAnnotMap,
					alreadyPrinted);
		}

		List<String> bertTrainingLines = new ArrayList<String>();
		bertTrainingLines.addAll(positiveBertTrainingLines);
		if (negativeBertTrainingLines != null) {
			bertTrainingLines.addAll(negativeBertTrainingLines);
		}

//		for (String sentence : positiveBertTrainingLines) {
//			System.out.println("POS MASK: " + sentence);
//		}
//		if (negativeBertTrainingLines != null) {
//			for (String sentence : negativeBertTrainingLines) {
//				System.out.println("NEG MASK: " + sentence);
//			}
//		}

//		labelToMaskedSentences.put(cp.getPredicateBiolink(), positiveBertTrainingLines);
//		labelToMaskedSentences.put("false", negativeBertTrainingLines);

		return bertTrainingLines;

	}

	private static Set<String> createNegativeTrainingLines(String sentenceText, BiolinkAssociation association,
			ConceptPair cp, Map<AnnotationType, Set<TextAnnotation>> typeToAnnotMap, Set<String> alreadyPrinted) {

		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();
		TextAnnotation sentenceAnnot = factory.createAnnotation(0, sentenceText.length(), sentenceText, "sentence");

		Set<String> trainingLines = new HashSet<String>();

		Set<TextAnnotation> positivePairAnnots = typeToAnnotMap.get(AnnotationType.SUBJECT_ANNOTATION);
		positivePairAnnots.addAll(typeToAnnotMap.get(AnnotationType.OBJECT_ANNOTATION));

		Set<TextAnnotation> subjectAnnots = typeToAnnotMap.get(AnnotationType.SUBJECT_ANNOTATION);
		subjectAnnots.addAll(typeToAnnotMap.get(AnnotationType.SUBJECT_TYPE_ANNOTATION));
		Set<TextAnnotation> objectAnnots = typeToAnnotMap.get(AnnotationType.OBJECT_ANNOTATION);
		objectAnnots.addAll(typeToAnnotMap.get(AnnotationType.OBJECT_TYPE_ANNOTATION));

		for (TextAnnotation subjectAnnot : subjectAnnots) {
			for (TextAnnotation objectAnnot : objectAnnots) {
				if (!(positivePairAnnots.contains(subjectAnnot) && positivePairAnnots.contains(objectAnnot))) {
					Assertion assertion = new Assertion(association.getSubjectClass(), subjectAnnot,
							association.getObjectClass(), objectAnnot, "false");
					boolean includeEntityTextInBertLine = true; // useful for manual review
					String trainingLine = BratToBertConverter.getTrainingExampleLine(sentenceAnnot, assertion,
							alreadyPrinted, includeEntityTextInBertLine);
					if (!alreadyPrinted.contains(trainingLine)) {
						alreadyPrinted.add(trainingLine);
						trainingLines.add(trainingLine);
					}
				}
			}
		}

		return trainingLines;

	}

	private static Set<String> createPositiveBertTrainingLines(String sentenceText, BiolinkAssociation association,
			ConceptPair cp, Map<AnnotationType, Set<TextAnnotation>> typeToAnnotMap, Set<String> alreadyPrinted) {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();
		TextAnnotation sentenceAnnot = factory.createAnnotation(0, sentenceText.length(), sentenceText, "sentence");

		Set<String> bertTrainingLines = new HashSet<String>();

		Set<TextAnnotation> subjectAnnots = typeToAnnotMap.get(AnnotationType.SUBJECT_ANNOTATION);
		Set<TextAnnotation> objectAnnots = typeToAnnotMap.get(AnnotationType.OBJECT_ANNOTATION);

		for (TextAnnotation subjectAnnot : subjectAnnots) {
			for (TextAnnotation objectAnnot : objectAnnots) {
				Assertion assertion = new Assertion(association.getSubjectClass(), subjectAnnot,
						association.getObjectClass(), objectAnnot, cp.getPredicateBiolink());
				boolean includeEntityTextInBertLine = true; // useful for manual review
				String trainingLine = BratToBertConverter.getTrainingExampleLine(sentenceAnnot, assertion,
						alreadyPrinted, includeEntityTextInBertLine);
				if (!alreadyPrinted.contains(trainingLine)) {
					alreadyPrinted.add(trainingLine);
					bertTrainingLines.add(trainingLine);
				}
			}
		}

		return bertTrainingLines;
	}

//	private static Set<String> getConceptIdsAboveIdfThreshold(float idfThreshold, File conceptIdfFile)
//			throws IOException {
//		Set<String> ids = new HashSet<String>();
//		for (StreamLineIterator lineIter = new StreamLineIterator(
//				new GZIPInputStream(new FileInputStream(conceptIdfFile)), CharacterEncoding.UTF_8, null); lineIter
//						.hasNext();) {
//			String line = lineIter.next().getText().replaceAll("\"", "");
//			String[] cols = line.split(",");
//			String id = cols[0];
//			String level = cols[1];
//			float idf = Float.parseFloat(cols[2]);
//
//			if (idf > idfThreshold && level.equals("document")) {
//				ids.add(id);
//			}
//		}
//
//		System.out.println("Ids that meet IDF threshold = " + ids.size());
//		return ids;
//	}

//	private static Map<String, Set<String>> loadAllowableGoConceptIds(File cachedFile, String namespace, File goOwlFile)
//			throws OWLOntologyCreationException, IOException {
//
//		Set<String> goIdsForNamespace = new HashSet<String>();
//		if (cachedFile.exists()) {
//			System.out.println("Loading from cached file: " + cachedFile.getAbsolutePath());
//			goIdsForNamespace.addAll(FileReaderUtil.loadLinesFromFile(cachedFile, ENCODING));
//		} else {
//			System.out.println("Loading... " + goOwlFile.getAbsolutePath());
//			OntologyUtil ontUtil = new OntologyUtil(goOwlFile);
//
//			try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(cachedFile)) {
//				for (Iterator<OWLClass> classIterator = ontUtil.getClassIterator(); classIterator.hasNext();) {
//					OWLClass cls = classIterator.next();
//					String ns = ontUtil.getNamespace(cls);
//					if (ns.endsWith("\"")) {
//						ns = StringUtil.removeLastCharacter(ns);
//					}
//					System.out.println("NS: " + ns);
//					if (ns.equals(namespace)) {
//						String iri = cls.getIRI().toString();
//						if (iri.contains("/")) {
//							String id = iri.substring(iri.lastIndexOf("/") + 1);
//							System.out.println("ID: " + id);
//							if (id.startsWith("GO_")) {
//								id = id.replace("_", ":");
//								goIdsForNamespace.add(id);
//								writer.write(id + "\n");
//							}
//						}
//					}
//				}
//			}
//		}
//		Map<String, Set<String>> map = new HashMap<String, Set<String>>();
//		map.put("GO", goIdsForNamespace);
//
//		System.out.println("Biological Process count: " + goIdsForNamespace.size());
//		System.out.println("Sample: " + goIdsForNamespace.iterator().next());
//
//		return map;
//	}
}
