package edu.cuanschutz.ccp.tm_provider.relation_extraction.distant_supervision;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.tools.ant.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;

import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import lombok.Getter;
import lombok.ToString;

public class ConceptPairsFileParser {

	private static final CharacterEncoding ENCODING = CharacterEncoding.UTF_8;

	private static final String OBO_NS = "http://purl.obolibrary.org/obo/";

	public static final String RO_HAS_PHENOTYPE = "http://purl.obolibrary.org/obo/RO_0002200";
	public static final String RO_LOCATED_IN = "http://purl.obolibrary.org/obo/RO_0001025";

	public static Set<ConceptPair> extractPositivePairs(File pairFile, String targetPredicateUri) throws IOException {
		InputStream is = (pairFile.getName().endsWith(".gz")) ? new GZIPInputStream(new FileInputStream(pairFile))
				: new FileInputStream(pairFile);

		Set<ConceptPair> pairs = new HashSet<ConceptPair>();

		for (StreamLineIterator lineIter = new StreamLineIterator(is, ENCODING, null); lineIter.hasNext();) {

			String text = lineIter.next().getText();
			if (!text.trim().isEmpty()) {
				String[] cols = text.split("\\t");
				String subjectUri = cols[0];
				String objectUri = cols[1];
				String predicateUri = cols[2];

				if (predicateUri.equals(targetPredicateUri)) {
					String subjectCurie = getCurie(subjectUri);
					String objectCurie = getCurie(objectUri);
					String predicateBiolink = getBiolink(predicateUri);
					pairs.add(new ConceptPair(subjectCurie, objectCurie, predicateBiolink));
				}
			}
		}

		return pairs;

	}

	/**
	 * @param pairFile
	 * @param targetPredicateUri
	 * @return pairs of MONOD/HP concepts that are not paired together in the file,
	 *         using the concept IDs that do appear in the file.
	 * @throws IOException
	 */
	public static Set<ConceptPair> extractNegativePairs(File pairFile, String targetPredicateUri) throws IOException {
		InputStream is = (pairFile.getName().endsWith(".gz")) ? new GZIPInputStream(new FileInputStream(pairFile))
				: new FileInputStream(pairFile);

		Set<String> objectCuries = new HashSet<String>();
		Map<String, Set<String>> positiveSubToObjMap = new HashMap<String, Set<String>>();

		for (StreamLineIterator lineIter = new StreamLineIterator(is, ENCODING, null); lineIter.hasNext();) {

			String text = lineIter.next().getText();
			if (!text.trim().isEmpty()) {
				String[] cols = text.split("\\t");
				String subjectUri = cols[0];
				String objectUri = cols[1];
				String predicateUri = cols[2];

				if (predicateUri.equals(targetPredicateUri)) {
					String subjectCurie = getCurie(subjectUri);
					String objectCurie = getCurie(objectUri);
					objectCuries.add(objectCurie);
					CollectionsUtil.addToOne2ManyUniqueMap(subjectCurie, objectCurie, positiveSubToObjMap);
				}
			}
		}

		// above we cataloged the positive links between subject and object curies. Now,
		// create negative links by linking the subject to any object that it wasn't
		// linked to in the positive cases

		int negObjCurieCount = 1000;

		int count = 0;
		Set<ConceptPair> negativeConceptPairs = new HashSet<ConceptPair>();
		for (Entry<String, Set<String>> entry : positiveSubToObjMap.entrySet()) {
			if (count++ % 100 == 0) {
				System.out.println(String.format("Progress: %d of %d", (count -1), positiveSubToObjMap.size()));
			}
			String subjectCurie = entry.getKey();

			Set<String> negObjCuries = new HashSet<String>(objectCuries);
			negObjCuries.removeAll(entry.getValue());

			// create ConceptPair objects that have 100 negObjCuries
			List<String> negObjCuriesList = new ArrayList<String>(negObjCuries);
			negativeConceptPairs.addAll(subdivideNegativeCuries(negObjCurieCount, subjectCurie, negObjCuriesList));
		}

		return negativeConceptPairs;

	}

	@VisibleForTesting
	protected static Set<ConceptPair> subdivideNegativeCuries(int negObjCurieCount, String subjectCurie,
			List<String> negObjCuriesList) {
		Set<ConceptPair> cpSet = new HashSet<ConceptPair>();
		while (!negObjCuriesList.isEmpty()) {
			List<String> subList = null;
			if (negObjCuriesList.size() > negObjCurieCount) {
				subList = new ArrayList<String>(negObjCuriesList.subList(0, negObjCurieCount));
				negObjCuriesList.removeAll(subList);
			} else {
				subList = new ArrayList<String>(negObjCuriesList);
				negObjCuriesList.removeAll(subList);
			}
			cpSet.add(new ConceptPair(subjectCurie, subList, "false"));
		}
		return cpSet;
	}

	private static String getBiolink(String uri) {
		switch (uri) {
		case RO_HAS_PHENOTYPE:
			return "biolink:has_phenotype";
		case RO_LOCATED_IN:
			return "biolink:located_in";

		default:
			throw new IllegalArgumentException("Unhandled predicate URI: " + uri);
		}

	}

	private static String getCurie(String uri) {
		String curie = StringUtils.removePrefix(uri, OBO_NS);
		curie = curie.replace("_", ":");
		return curie;
	}

	@Getter
	@ToString
	public static class ConceptPair {
		private final String subjectCurie;
		private final Set<String> objectCuries;
		private final String predicateBiolink;

		public ConceptPair(String subjectCurie, String objectCurie, String predicateBiolink) {
			this.subjectCurie = subjectCurie;
			this.objectCuries = new HashSet<String>();
			this.objectCuries.add(objectCurie);
			this.predicateBiolink = predicateBiolink;
		}

		public ConceptPair(String subjectCurie, Collection<String> objectCuries, String predicateBiolink) {
			this.subjectCurie = subjectCurie;
			this.objectCuries = new HashSet<String>(objectCuries);
			this.predicateBiolink = predicateBiolink;
		}

	}

}
