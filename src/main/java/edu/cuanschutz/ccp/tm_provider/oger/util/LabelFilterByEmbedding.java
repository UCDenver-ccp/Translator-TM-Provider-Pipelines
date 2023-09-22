package edu.cuanschutz.ccp.tm_provider.oger.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import edu.cuanschutz.ccp.tm_provider.oger.util.EmbeddingUtil.Embedding;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.collections.CollectionsUtil.SortOrder;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileReaderUtil;
import edu.ucdenver.ccp.common.file.reader.Line;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;

/**
 * Takes an ontology as input and builds an average embedding for a given
 * domain, e.g., chemicals. Then allow for filtering of labels based on
 * embedding distance of labels that are far from the average, e.g., things that
 * don't seem like chemicals.
 *
 */
public class LabelFilterByEmbedding {

	public static double[] createAverageEmbedding(File ontologyFile, String rootClassId,
			Map<String, Embedding> embeddingsMap) throws OWLOntologyCreationException {
		OntologyUtil ontUtil = new OntologyUtil(ontologyFile);

		OWLClass owlClass = ontUtil.getOWLClassFromId(rootClassId);
		Set<OWLClass> descendents = ontUtil.getDescendents(owlClass);

		int descendentCount = descendents.size();
		int index = 0;

		double[] averageEmbedding = null;

		for (OWLClass descendent : descendents) {
			if (index++ % 100 == 0) {
				System.out.println(String.format("Progress: %d of %d", (index - 1), descendentCount));
			}
			String label = ontUtil.getLabel(descendent);
			// only use single token labels
			String[] toks = label.split(" ");
			if (toks.length == 1) {
				if (embeddingsMap.containsKey(label)) {
					Embedding embedding = embeddingsMap.get(label);
					if (averageEmbedding == null) {
						averageEmbedding = embedding.getEmbeddingVector();
					} else {
						averageEmbedding = EmbeddingUtil.average(averageEmbedding, embedding.getEmbeddingVector());
					}
				}
			}

//			for (String labelPart : label.split(" ")) {
//				if (embeddingsMap.containsKey(labelPart)) {
//			if (embeddingsMap.containsKey(label)) {
//					Embedding embedding = embeddingsMap.get(labelPart);
//					if (averageEmbedding == null) {
//						averageEmbedding = embedding.getEmbeddingVector();
//					} else {
//						averageEmbedding = EmbeddingUtil.average(averageEmbedding, embedding.getEmbeddingVector());
//					}
//				}
//		}
//			}
		}
		return averageEmbedding;
	}

	/**
	 * There are labels in the MolePro chemical file used by multiple identifiers,
	 * e.g., triglyceride. Here we look at those that are associated with a high
	 * number of different chemicals to see if adjustments need to be made.
	 * 
	 * @param moleproLabelsFile
	 * @throws IOException
	 */
	private static void findOverlappingLabels(File moleproLabelsFile) throws IOException {
		Map<String, Set<String>> labelToIdsMap = new HashMap<String, Set<String>>();
		for (StreamLineIterator lineIter = new StreamLineIterator(moleproLabelsFile, CharacterEncoding.UTF_8); lineIter
				.hasNext();) {
			Line line = lineIter.next();
			String[] cols = line.getText().split("\\t");
			String id = cols[0];
			String label = cols[1];
			CollectionsUtil.addToOne2ManyUniqueMap(label, id, labelToIdsMap);
		}

		System.out.println("Unique label count = " + labelToIdsMap.size());

		Set<String> toRemove = new HashSet<String>();
		for (Entry<String, Set<String>> entry : labelToIdsMap.entrySet()) {
			if (entry.getValue().size() == 1) {
				toRemove.add(entry.getKey());
			}
		}

		System.out.println("Labels associated with a single identifier: " + toRemove.size());

		for (String key : toRemove) {
			labelToIdsMap.remove(key);
		}

		System.out.println("Labels associated with multiple identifiers: " + labelToIdsMap.size());

		Map<String, Integer> labelToIdCountMap = new HashMap<String, Integer>();
		for (Entry<String, Set<String>> entry : labelToIdsMap.entrySet()) {
			labelToIdCountMap.put(entry.getKey(), entry.getValue().size());
		}

		Map<String, Integer> sortedLabelToIdCountMap = CollectionsUtil.sortMapByValues(labelToIdCountMap,
				SortOrder.DESCENDING);

		int count = 0;
		for (Entry<String, Integer> entry : sortedLabelToIdCountMap.entrySet()) {
			List<String> labels = new ArrayList<String>(labelToIdsMap.get(entry.getKey()));
			if (labels.size() > 20) {
				labels = labels.subList(0, 20);
			}
			System.out.println(String.format("%s\t%d\t%s", entry.getKey(), entry.getValue(), labels));

			if (count++ > 100) {
				break;
			}
		}

	}

	/**
	 * I wrote this while trying to track down the overlapping labels in
	 * PUBCHEM.COMPOUND:441922 `Ginsenoside Rf` and PUBCHEM.COMPOUND:522017
	 * `Triarachidin`. This method takes a list of labels and only finds overlaps
	 * for that list.
	 * 
	 * @param moleproLabelsFile
	 * @throws IOException
	 */
	private static void findSpecificOverlappingLabels(File moleproLabelsFile, List<String> specificLabels)
			throws IOException {

		Set<String> spLabels = new HashSet<String>();
		for (String label : specificLabels) {
			spLabels.add(label.toLowerCase());
		}

		Map<String, Set<String>> labelToIdsMap = new HashMap<String, Set<String>>();
		for (StreamLineIterator lineIter = new StreamLineIterator(moleproLabelsFile, CharacterEncoding.UTF_8); lineIter
				.hasNext();) {
			Line line = lineIter.next();
			String[] cols = line.getText().split("\\t");
			String id = cols[0];
			String label = cols[1].toLowerCase();
			if (spLabels.contains(label)) {
				CollectionsUtil.addToOne2ManyUniqueMap(label, id, labelToIdsMap);
			}
		}
		System.out.println("Unique label count = " + labelToIdsMap.size());

		Set<String> toRemove = new HashSet<String>();
		for (Entry<String, Set<String>> entry : labelToIdsMap.entrySet()) {
			if (entry.getValue().size() == 1) {
				toRemove.add(entry.getKey());
			}
		}

		System.out.println("Labels associated with a single identifier: " + toRemove.size());

		for (String key : toRemove) {
			labelToIdsMap.remove(key);
		}

		System.out.println("Labels associated with multiple identifiers: " + labelToIdsMap.size());

		Map<String, Integer> labelToIdCountMap = new HashMap<String, Integer>();
		for (Entry<String, Set<String>> entry : labelToIdsMap.entrySet()) {
			labelToIdCountMap.put(entry.getKey(), entry.getValue().size());
		}

		Map<String, Integer> sortedLabelToIdCountMap = CollectionsUtil.sortMapByValues(labelToIdCountMap,
				SortOrder.DESCENDING);

		int count = 0;
		for (Entry<String, Integer> entry : sortedLabelToIdCountMap.entrySet()) {
			List<String> labels = new ArrayList<String>(labelToIdsMap.get(entry.getKey()));
			if (labels.size() > 20) {
				labels = labels.subList(0, 20);
			}
			System.out.println(String.format("%s\t%d\t%s", entry.getKey(), entry.getValue(), labels));

			if (count++ > 100) {
				break;
			}
		}

	}

	public static void main(String[] args) {
		String rootClassId = "http://purl.obolibrary.org/obo/CHEBI_24431";
		File ontologyFile = new File(
				"/Users/bill/projects/ncats-translator/concept-recognition/embeddings/bert-crel/chebi.owl");
		File embeddingFile = new File(
				"/Users/bill/projects/ncats-translator/concept-recognition/embeddings/bert-crel/BERT-CRel-words.vec.gz");
		File averageEmbeddingFile = new File(
				"/Users/bill/projects/ncats-translator/concept-recognition/embeddings/bert-crel/chemical.avg.embedding");
		File moleproLabelsFile = new File(
				"/Users/bill/projects/ncats-translator/concept-recognition/chemicals-from-molepro/molepro.chemical.labels.tsv");
		try {

			findLabelsThatAreEnglishWords(moleproLabelsFile);

			/*
			 * the findOverlappingLabels methods were used to analyze the Molepro file for
			 * overlapping labels
			 */
			if (false) {
//			findOverlappingLabels(moleproLabelsFile);

				List<String> specificLabels = Arrays.asList("panaxoside rf", "Panaxoside RF",
						"2-[2-[[3,12-dihydroxy-17-(2-hydroxy-6-methylhept-5-en-2-yl)-4,4,8,10,14-pentamethyl-2,3,5,6,7,9,11,12,13,15,16,17-dodecahydro-1H-cyclopenta[a]phenanthren-6-yl]oxy]-4,5-dihydroxy-6-(hydroxymethyl)oxan-3-yl]oxy-6-(hydroxymethyl)oxane-3,4,5-triol",
						"LS-15446", "Ginsenoside Rf",
						"2-[(2-{[5,16-dihydroxy-14-(2-hydroxy-6-methylhept-5-en-2-yl)-2,6,6,10,11-pentamethyltetracyclo[8.7.0.0²,⁷.0¹¹,¹⁵]heptadecan-8-yl]oxy}-4,5-dihydroxy-6-(hydroxymethyl)oxan-3-yl)oxy]-6-(hydroxymethyl)oxane-3,4,5-triol",
						"2-[(2-{[5,16-dihydroxy-14-(2-hydroxy-6-methylhept-5-en-2-yl)-2,6,6,10,11-pentamethyltetracyclo[8.7.0.0²,⁷.0¹¹,¹⁵]heptadecan-8-yl]oxy}-4,5-dihydroxy-6-(hydroxymethyl)oxan-3-yl)oxy]-6-(hydroxymethyl)oxane-3,4,5-triol",
						"einecs 210-646-7", "Triarachidin", "2,3-di(icosanoyloxy)propyl icosanoate",
						"triarachidoylglycerol", "AKOS037645043", "48rpr95fw8", "48RPR95FW8", "Q27259169",
						"DTXSID901018697", "CS-0092967", "T1389", "HY-125671", "triarachin", "AS-56408",
						"ZINC150340070", "FT-0710378", "SCHEMBL9754706", "T-6000", "T72717", "unii-48rpr95fw8",
						"einecs 210-646-7", "TG(20:0/20:0/20:0)", "1,3-bis(icosanoyloxy)propan-2-yl icosanoate",
						"triglyceride", "TRIARACHIDIN");

				findSpecificOverlappingLabels(moleproLabelsFile, specificLabels);
			}

			if (false) {
				Map<String, Embedding> embeddingsMap = EmbeddingUtil.loadEmbeddingFile(embeddingFile, " ");
//			double[] averageEmbedding = createAverageEmbedding(ontologyFile, rootClassId, embeddingsMap);
//			EmbeddingUtil.serializeEmbedding(averageEmbeddingFile, averageEmbedding);

				double[] avgEmbedding = EmbeddingUtil.deserializeEmbedding(averageEmbeddingFile);

				double[] focusEmbedding = embeddingsMap.get("focus").getEmbeddingVector();
				double[] optimalEmbedding = embeddingsMap.get("optimal").getEmbeddingVector();
				double[] solutionEmbedding = embeddingsMap.get("solution").getEmbeddingVector();
				double[] metforminEmbedding = embeddingsMap.get("metformin").getEmbeddingVector();
				double[] benzeneEmbedding = embeddingsMap.get("benzene").getEmbeddingVector();
				double[] heartEmbedding = embeddingsMap.get("heart").getEmbeddingVector();
				double[] acidEmbedding = embeddingsMap.get("acid").getEmbeddingVector();
				double[] ligandEmbedding = embeddingsMap.get("ligand").getEmbeddingVector();

				double cosineSimilarity = EmbeddingUtil.cosineSimilarity(avgEmbedding, focusEmbedding);
				System.out.println(String.format("Cosine similarity 'focus': %f", cosineSimilarity));
				cosineSimilarity = EmbeddingUtil.cosineSimilarity(avgEmbedding, optimalEmbedding);
				System.out.println(String.format("Cosine similarity 'optimal': %f", cosineSimilarity));
				cosineSimilarity = EmbeddingUtil.cosineSimilarity(avgEmbedding, solutionEmbedding);
				System.out.println(String.format("Cosine similarity 'solution': %f", cosineSimilarity));
				cosineSimilarity = EmbeddingUtil.cosineSimilarity(avgEmbedding, metforminEmbedding);
				System.out.println(String.format("Cosine similarity 'metformin': %f", cosineSimilarity));
				cosineSimilarity = EmbeddingUtil.cosineSimilarity(avgEmbedding, benzeneEmbedding);
				System.out.println(String.format("Cosine similarity 'benzene': %f", cosineSimilarity));
				cosineSimilarity = EmbeddingUtil.cosineSimilarity(avgEmbedding, heartEmbedding);
				System.out.println(String.format("Cosine similarity 'heart': %f", cosineSimilarity));
				cosineSimilarity = EmbeddingUtil.cosineSimilarity(avgEmbedding, acidEmbedding);
				System.out.println(String.format("Cosine similarity 'acid': %f", cosineSimilarity));
				cosineSimilarity = EmbeddingUtil.cosineSimilarity(avgEmbedding, ligandEmbedding);
				System.out.println(String.format("Cosine similarity 'ligand': %f", cosineSimilarity));

//			for (StreamLineIterator lineIter = new StreamLineIterator(moleproLabelsFile,
//					CharacterEncoding.UTF_8); lineIter.hasNext();) {
//				Line line = lineIter.next();
//				String[] cols = line.getText().split("\\t");
//				String id = cols[0];
//				String label = cols[1];
//				if (label.indexOf(" ") < 0 && embeddingsMap.containsKey(label)) {
//					cosineSimilarity = EmbeddingUtil.cosineSimilarity(avgEmbedding,
//							embeddingsMap.get(label).getEmbeddingVector());
//					if (cosineSimilarity < 0.1) {
//						System.out.println(String.format("Possible bad label: %s -- %s (%f)",id,  label, cosineSimilarity));
//					}
//				}
//
//			}

			}

		} catch (IOException e) {// | OWLOntologyCreationException e) {
			e.printStackTrace();
		}

	}

	private static void findLabelsThatAreEnglishWords(File moleproLabelsFile) throws IOException {
		/*
		 * 6/28/23 -- use the below to find potentially troublesome labels by matching
		 * to a dictionary of english words
		 */
		File wordsAlphaFile = new File(
				"/Users/bill/projects/ncats-translator/concept-recognition/embeddings/bert-crel/words_alpha.txt");
		List<String> words = FileReaderUtil.loadLinesFromFile(wordsAlphaFile, CharacterEncoding.UTF_8);
		Set<String> wordsSet = new HashSet<String>(words);
		for (StreamLineIterator lineIter = new StreamLineIterator(moleproLabelsFile, CharacterEncoding.UTF_8); lineIter
				.hasNext();) {
			Line line = lineIter.next();
			String[] cols = line.getText().split("\\t");
			String id = cols[0];
			String label = cols[1];
			if (wordsSet.contains(label)) {
				System.out.println(String.format("WORD Possible bad label: %s -- %s", id, label));
			}

		}
	}

}
