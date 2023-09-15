package edu.cuanschutz.ccp.tm_provider.oger.dict;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import edu.cuanschutz.ccp.tm_provider.oger.util.OgerDictFileFactory;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileReaderUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.reader.Line;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import edu.ucdenver.ccp.common.string.StringUtil;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;

public class PrOgerDictFileFactory extends OgerDictFileFactory {

	private static final String MGI_PREFIX = "http://www.informatics.jax.org/marker/";

	private static final String HGNC_PREFIX = "http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=";

	private static final String RGD_PREFIX = "http://rgd.mcw.edu/rgdweb/report/gene/main.html?id=";

	private static final String HAS_GENE_TEMPLATE_PROPERTY_IRI = "http://purl.obolibrary.org/obo/pr#has_gene_template";

	private static final String OBO_PURL = "http://purl.obolibrary.org/obo/";

	private static final String ONTOLOGY_PREFIX = "PR";

	private static final Object ONLY_IN_TAXON_IRI = "http://purl.obolibrary.org/obo/RO_0002160";

	private final Set<String> englishWords;
	private BufferedWriter tmpWriter;
//	private final Map<String, Set<String>> iriToExcludedEnglishWordLabels;
	private final Set<String> toExcludedEnglishWordLabels;
	private final Map<String, Set<String>> ncbiGeneIdToGeneSynonymsMap;

	/**
	 * @param englishWordFile
	 * @param englishWordMatchLogFile
	 * @param excludeEnglishWordFile  - this file has been manually curated to
	 *                                exclude labels that are likely false positives
	 * @throws IOException
	 */
	public PrOgerDictFileFactory(File englishWordFile, File englishWordMatchLogFile, File excludeEnglishWordFile,
			File ncbiGeneInfoFile) throws IOException {
		super("protein", "PR", SynonymSelection.EXACT_ONLY, null);
		System.out.print("Loading English words...");
		List<String> words = FileReaderUtil.loadLinesFromFile(englishWordFile, CharacterEncoding.UTF_8);
		englishWords = new HashSet<String>(words);
		System.out.println("complete.");
		toExcludedEnglishWordLabels = loadExcludedEnglishWordLabels(excludeEnglishWordFile);
		ncbiGeneIdToGeneSynonymsMap = loadNcbiGeneIdToGeneSynonymsMap(ncbiGeneInfoFile);
		tmpWriter = FileWriterUtil.initBufferedWriter(englishWordMatchLogFile);
	}

	@Override
	public void createOgerDictionaryFile(File ontologyFile, File dictDirectory) throws IOException {
//		Set<String> genePrefixes = new HashSet<String>();
		System.out.print("Loading ontology...");
		OntologyUtil ontUtil;
		try {
			ontUtil = new OntologyUtil(new GZIPInputStream(new FileInputStream(ontologyFile)));
		} catch (OWLOntologyCreationException e) {
			throw new IOException(e);
		}
		System.out.println("complete.");

		// PR families get sent to the max_normfile
		File caseInsensitiveMaxNormDictFile = new File(dictDirectory,
				String.format("%s.case_insensitive_max_norm.tsv", ONTOLOGY_PREFIX));
		File caseInsensitiveMinNormDictFile = new File(dictDirectory,
				String.format("%s.case_insensitive_min_norm.tsv", ONTOLOGY_PREFIX));
		File caseSensitiveDictFile = new File(dictDirectory, String.format("%s.case_sensitive.tsv", ONTOLOGY_PREFIX));

		try (BufferedWriter caseSensWriter = FileWriterUtil.initBufferedWriter(caseSensitiveDictFile);
				BufferedWriter caseInsensMinNormWriter = FileWriterUtil
						.initBufferedWriter(caseInsensitiveMinNormDictFile);
				BufferedWriter caseInsensMaxNormWriter = FileWriterUtil
						.initBufferedWriter(caseInsensitiveMaxNormDictFile)) {

			Set<OWLClass> skipped = new HashSet<OWLClass>();
			Set<OWLClass> used = new HashSet<OWLClass>();

			int count = 0;
			for (Iterator<OWLClass> classIterator = ontUtil.getClassIterator(); classIterator.hasNext();) {
				if (count++ % 10000 == 0) {
					System.out.println(String.format("progress: %d...", count - 1));
				}
				OWLClass cls = classIterator.next();

				if (cls.getIRI().toString().contains(ONTOLOGY_PREFIX)) {
					skipped.add(cls);
					if (isFamilyLevel(cls, ontUtil)) {
						// we create a dictionary entry for family-level concepts, but do not include
						// synonyms from their descendants
						used.add(cls);
						Map<String, Set<String>> synonymToSourceMap = new HashMap<String, Set<String>>();
						extractSynonyms(ontUtil, synonymToSourceMap, cls);
						writeDictLines(cls, synonymToSourceMap, caseSensWriter, caseInsensMaxNormWriter);
					} else if (isGeneLevel(cls, ontUtil)) {

						// if this class has gene-level children, then skip
						if (hasGeneLevelChildren(cls, ontUtil)) {
							used.add(cls);
							continue;
						}

						used.add(cls);
						Map<String, Set<String>> synonymToSourceMap = new HashMap<String, Set<String>>();
						extractSynonyms(ontUtil, synonymToSourceMap, cls);

						// get descendants
						Set<OWLClass> descendants = ontUtil.getDescendents(cls);
						// for each descendant, get exact synonyms
						for (OWLClass descendant : descendants) {
							used.add(descendant);
							extractSynonyms(ontUtil, synonymToSourceMap, descendant);
						}

						writeDictLines(cls, synonymToSourceMap, caseSensWriter, caseInsensMinNormWriter);
//						Set<String> caseInsensitiveSyns = new HashSet<String>(synonymToSourceMap.keySet());
//						Set<String> caseSensitiveSyns = getCaseSensitiveSynonyms(caseInsensitiveSyns);
//						/* the synonyms set becomes the case-insensitive set */
//						caseInsensitiveSyns.removeAll(caseSensitiveSyns);
//
//						for (String syn : caseSensitiveSyns) {
//							String label = CollectionsUtil.createDelimitedString(synonymToSourceMap.get(syn), "|");
//							if (label == null) {
//								label = "CS_addition";
//							}
//							caseSensWriter.write(
//									getDictLine(ONTOLOGY_PREFIX, cls.getIRI().toString(), syn, label, "protein", true));
//						}
//						for (String syn : caseInsensitiveSyns) {
//							String label = CollectionsUtil.createDelimitedString(synonymToSourceMap.get(syn), "|");
//							if (label == null) {
//								label = "CI_addition";
//							}
//							caseInsensWriter.write(
//									getDictLine(ONTOLOGY_PREFIX, cls.getIRI().toString(), syn, label, "protein", true));
//						}
					}
				}

//			Set<OWLClass> ancestors = ontUtil.getAncestors(cls);
//			// if there is an ancestor at the Category=gene level, then return its id
//			Set<OWLClass> geneLevelAncestors = new HashSet<OWLClass>();
//			for (OWLClass ancestorCls : ancestors) {
//				if (isGeneLevel(ancestorCls)) {
//					geneLevelAncestors.add(ancestorCls);
//				}
//			}
//
//			// if there is only one gene-level ancestor, then that's the one we want.
//			// Otherwise, we want the lowest level (farthest from the root) gene-level
//			// ancestor
//			if (!geneLevelAncestors.isEmpty()) {
//				OWLClass geneLevelCls = geneLevelAncestors.iterator().next();
//				if (geneLevelAncestors.size() > 1) {
//					for (OWLClass cls1 : geneLevelAncestors) {
//						if (!cls1.equals(geneLevelCls)) {
//							Set<OWLClass> ancestors1 = ontUtil.getAncestors(cls1);
//							if (ancestors1.contains(geneLevelCls)) {
//								geneLevelCls = cls1;
//							}
//						}
//					}
//				}
//
//				writeMapping(writer, getId(cls), getId(geneLevelCls));
//
//			}
			}

			skipped.removeAll(used);

			// are there any skipped that are human?
			for (OWLClass skippedCls : skipped) {
				Map<String, Set<String>> outgoingEdges = ontUtil.getOutgoingEdges(skippedCls);

				if (outgoingEdges.containsKey(ONLY_IN_TAXON_IRI)) {
					Set<String> values = outgoingEdges.get(ONLY_IN_TAXON_IRI);
					if (values.contains("http://purl.obolibrary.org/obo/NCBITaxon_9606")) {
						System.out.println("Adding skipped human protein: " + skippedCls.getIRI().toString());

						Map<String, Set<String>> synonymToSourceMap = new HashMap<String, Set<String>>();
						extractSynonyms(ontUtil, synonymToSourceMap, skippedCls);
						writeDictLines(skippedCls, synonymToSourceMap, caseSensWriter, caseInsensMinNormWriter);

					}

				}
			}

		}

//		File file = new File("/tmp/gene-prefixes.out");
//		try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(file)) {
//			List<String> prefixList = new ArrayList<String>(genePrefixes);
//			Collections.sort(prefixList);
//			for (String prefix : prefixList) {
//				writer.write(String.format("%s\n", prefix));
//			}
//
//		}

	}

	private void writeDictLines(OWLClass cls, Map<String, Set<String>> synonymToSourceMap,
			BufferedWriter caseSensWriter, BufferedWriter caseInsensWriter) throws IOException {
		Set<String> caseInsensitiveSyns = new HashSet<String>(synonymToSourceMap.keySet());
		Set<String> caseSensitiveSyns = getCaseSensitiveSynonyms(caseInsensitiveSyns);
		/* the synonyms set becomes the case-insensitive set */
		caseInsensitiveSyns.removeAll(caseSensitiveSyns);

		for (String syn : caseSensitiveSyns) {
			String label = CollectionsUtil.createDelimitedString(synonymToSourceMap.get(syn), "|");
			if (label == null) {
				label = "CS_addition";
			}
			caseSensWriter
					.write(getDictLine(ONTOLOGY_PREFIX, cls.getIRI().toString(), syn, label, "protein", true, null));
		}
		for (String syn : caseInsensitiveSyns) {
			String label = CollectionsUtil.createDelimitedString(synonymToSourceMap.get(syn), "|");
			if (label == null) {
				label = "CI_addition";
			}
			caseInsensWriter
					.write(getDictLine(ONTOLOGY_PREFIX, cls.getIRI().toString(), syn, label, "protein", true, null));
		}
	}

	private void extractSynonyms(OntologyUtil ontUtil, Map<String, Set<String>> synonymToSourceMap,
			OWLClass descendant) {
		String label = ontUtil.getLabel(descendant);
		Set<String> synonyms = OgerDictFileFactory.getSynonyms(ontUtil, descendant, label, SynonymSelection.EXACT_ONLY);
		synonyms = augmentSynonyms(descendant.getIRI().toString(), synonyms, ontUtil);
		for (String syn : synonyms) {
			CollectionsUtil.addToOne2ManyUniqueMap(syn, getId(descendant), synonymToSourceMap);
		}
	}

//	http://purl.obolibrary.org/obo/PR_Q8TD3
//    <rdfs:subClassOf rdf:resource="http://purl.obolibrary.org/obo/PR_000008209"/>
//    <rdfs:subClassOf rdf:resource="http://purl.obolibrary.org/obo/PR_000050341"/>

	private Set<String> getPrefixes(Set<String> values) {
		Set<String> prefixes = new HashSet<String>();
		for (String val : values) {
			int hashIndex = val.lastIndexOf("#");
			int equalIndex = val.lastIndexOf("=");
			int slashIndex = val.lastIndexOf("/");
			int lastIndex = Math.max(hashIndex, Math.max(equalIndex, slashIndex));
			String prefix = val.substring(0, lastIndex);
			prefixes.add(prefix);
		}
		return prefixes;
	}

	/**
	 * @param cls
	 * @param ontUtil
	 * @return true if the input class has gene-level children, false otherwise
	 */
	private boolean hasGeneLevelChildren(OWLClass cls, OntologyUtil ontUtil) {
		for (OWLClass descendent : ontUtil.getDescendents(cls)) {
			if (isGeneLevel(descendent, ontUtil)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Convert from full URI to PREFIX:00000
	 * 
	 * @param cls
	 * @param replacedEXT
	 * @return
	 */
	private String getId(OWLClass cls) {
		String iri = cls.getIRI().toString();
		int index = iri.lastIndexOf("/") + 1;
		return iri.substring(index).replace("_", ":");
	}

	private boolean isFamilyLevel(OWLClass cls, OntologyUtil ontUtil) {
		List<String> comments = ontUtil.getComments(cls);
		if (comments != null && !comments.isEmpty()) {
			for (String comment : comments) {
				if (comment.contains("Category=family.")) {
					return true;
				}
			}
		}
		return false;
	}

	private boolean isGeneLevel(OWLClass cls, OntologyUtil ontUtil) {
		List<String> comments = ontUtil.getComments(cls);
		if (comments != null && !comments.isEmpty()) {
			for (String comment : comments) {
				if (comment.contains("Category=gene.")) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * 
	 * <pre>
	 * 0 tax_id:
	 * 1 GeneID:
	 * 2 Symbol:
	 * 3 LocusTag:
	 * 4 Synonyms:
	       bar-delimited set of unofficial symbols for the gene
	 * 5 dbXrefs:
	 * 6 chromosome:
	 * 7 map location:
	 * 8 description:
	 * 9 type of gene:
	 * 10 Symbol from nomenclature authority:
	 * 11 Full name from nomenclature authority:
	 * 12 Nomenclature status:
	 * 13 Other designations:
	 * 14 Modification date:
	 * 15 Feature type:
	 * </pre>
	 * 
	 * @param ncbiGeneInfoFile
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private Map<String, Set<String>> loadNcbiGeneIdToGeneSynonymsMap(File ncbiGeneInfoFile)
			throws FileNotFoundException, IOException {

		Map<String, Set<String>> map = new HashMap<String, Set<String>>();

		for (StreamLineIterator lineIter = new StreamLineIterator(
				new GZIPInputStream(new FileInputStream(ncbiGeneInfoFile)), CharacterEncoding.UTF_8, null); lineIter
						.hasNext();) {
			Line line = lineIter.next();
			if (line.getLineNumber() % 1000000 == 0) {
				System.out.println(String.format("gene_info parse progress: %d", line.getLineNumber()));
			}

			String[] cols = line.getText().split("\\t");
			if (cols.length > 11) {

//			String geneId = cols[1];
				String symbol = cols[2];
				List<String> synonyms = Arrays.asList(cols[4].split("\\|"));
				String symbolFromNomenclatureAuthority = cols[10];
				String fullNameFromNomenclatureAuthority = cols[11];
				List<String> dbXrefs = Arrays.asList(cols[5].split("\\|"));
				String id = getId(dbXrefs);

				if (id != null) {
					Set<String> syns = new HashSet<String>();
					syns.add(symbol);
					syns.addAll(synonyms);
					syns.add(symbolFromNomenclatureAuthority);
					syns.add(fullNameFromNomenclatureAuthority);
					syns.remove("-");

					map.put(id, syns);
				}
			} else {
				System.out.println("skipping line with unexpected column count: " + line.getText());
			}

		}

		return map;

	}

	/**
	 * If there is a HGNC or an MGI id in the list then return it, otherwise return
	 * null
	 * 
	 * @param dbXrefs
	 * @return
	 */
	private String getId(List<String> dbXrefs) {
		for (String xref : dbXrefs) {
			if (xref.matches("HGNC:HGNC:\\d+")) {
				return StringUtil.removePrefix(xref, "HGNC:");
			} else if (xref.matches("MGI:MGI:\\d+")) {
				return StringUtil.removePrefix(xref, "MGI:");
			} else if (xref.matches("RGD:\\d+")) {
				return xref;
			}
		}
		return null;
	}

	/**
	 * Loads a map from IRI to English word labels that should be excluded (from a
	 * manually curated file)
	 * 
	 * @param excludeEnglishWordFile
	 * @return
	 * @throws IOException
	 */
	private Set<String> loadExcludedEnglishWordLabels(File excludeEnglishWordFile) throws IOException {
//		Map<String, Set<String>> map = new HashMap<String, Set<String>>();
		Set<String> toExclude = new HashSet<String>();

		for (StreamLineIterator lineIter = new StreamLineIterator(excludeEnglishWordFile,
				CharacterEncoding.UTF_8); lineIter.hasNext();) {
			Line line = lineIter.next();
			String[] cols = line.getText().split("\\t");
			String excludeFlag = cols[0];
//			String lowercaseLabel = cols[1];
			String label = cols[2];
			String iri = OBO_PURL + cols[3];

			if (excludeFlag.contains("x")) {
//				CollectionsUtil.addToOne2ManyUniqueMap(iri, label, map);
				toExclude.add(label);
			}
		}
		return toExclude;
	}

	public void close() throws IOException {
		tmpWriter.close();
	}

	public static final Set<String> EXCLUDED_INDIVIDUAL_CLASSES = new HashSet<String>(
			Arrays.asList(OBO_PURL + "PR_000000001", // protein
					OBO_PURL + "PR_000003507" // chimeric protein
			));

	@Override
	protected Set<String> augmentSynonyms(String iri, Set<String> syns, OntologyUtil ontUtil) {
		Set<String> toReturn = removeStopWords(syns);

		toReturn = addGeneSynonyms(iri, toReturn, ontUtil, ncbiGeneIdToGeneSynonymsMap);
		toReturn = removeWordsLessThenLength(toReturn, 3);
		toReturn = filterSpecificSynonyms(iri, toReturn);
		// log matches to english words for further manual curation
		logEnglishWords(iri, toReturn);
		// remove matches to english words that have been manually indicated for
		// exclusion
		toReturn = filterEnglishWordLabels(iri, toReturn);

		if (EXCLUDED_INDIVIDUAL_CLASSES.contains(iri)) {
			toReturn = Collections.emptySet();
		}

		return toReturn;
	}

	/**
	 * Look for a has_gene_template relationship and follow it to the corresponding
	 * gene - then lookup the gene synonyms that were mined from the NCBI gene_info
	 * file.
	 * 
	 * @param iri
	 * @param toReturn
	 * @param ontUtil
	 * @return
	 */
	protected static Set<String> addGeneSynonyms(String iri, Set<String> inputSyns, OntologyUtil ontUtil,
			Map<String, Set<String>> geneIdToSynonymsMap) {

		Set<String> toReturn = new HashSet<String>(inputSyns);

		OWLClass proteinCls = ontUtil.getOWLClassFromId(iri);

		Map<String, Set<String>> outgoingEdges = ontUtil.getOutgoingEdges(proteinCls);

		if (outgoingEdges.containsKey(HAS_GENE_TEMPLATE_PROPERTY_IRI)) {
			Set<String> values = outgoingEdges.get(HAS_GENE_TEMPLATE_PROPERTY_IRI);
			String geneId = parseGeneId(values);

			if (geneId != null) {
				if (geneIdToSynonymsMap.containsKey(geneId)) {
					toReturn.addAll(geneIdToSynonymsMap.get(geneId));
				}
			}
		}

		return toReturn;

	}

	/**
	 * Parse the HGNC or NCBI Gene ID from the input values, e.g.,
	 * http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=10002
	 * http://www.informatics.jax.org/marker/MGI:2151253
	 * http://purl.obolibrary.org/obo/PR_000028186
	 * 
	 * We map the following gene prefixes to NCBI gene in order to extract gene
	 * synonyms: http://rgd.mcw.edu/rgdweb/report/gene/main.html?id=2323 RGD:2323
	 * http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id
	 * http://www.informatics.jax.org/marker
	 * 
	 * 
	 * 
	 * 
	 * 
	 * Other prefixes in use that we aren't mapping to NCBI Gene for gene synonym
	 * extraction: http://birdgenenames.org/cgnc/GeneReport?id=10048 CGNC:10048
	 * http://www.ncbi.nlm.nih.gov/gene/100000930
	 * http://www.pombase.org/spombe/result/SPAC1834.04
	 * http://www.wormbase.org/species/c_elegans/gene/WBGene00000464
	 * http://www.yeastgenome.org/locus/S000001952
	 * http://zfin.org/action/marker/view/ZDB-GENE-110411-53
	 * http://dictybase.org/gene http://flybase.org/reports
	 * http://purl.obolibrary.org/obo http://purl.obolibrary.org/obo/Ensembl
	 * http://purl.obolibrary.org/obo/EnsemblBacteria http://www.ecogene.org/gene
	 * http://www.ensembl.org/id http://www.ensemblgenomes.org/id
	 * https://bar.utoronto.ca/thalemine/portal.do?externalids
	 * 
	 * 
	 * @param values
	 * @return
	 */
	private static String parseGeneId(Set<String> values) {
		for (String val : values) {
			if (val.startsWith(HGNC_PREFIX)) {
				return "HGNC:" + StringUtil.removePrefix(val, HGNC_PREFIX);
			}
			if (val.startsWith(MGI_PREFIX)) {
				return StringUtil.removePrefix(val, MGI_PREFIX);
			}
			if (val.startsWith(RGD_PREFIX)) {
				return "RGD:" + StringUtil.removePrefix(val, RGD_PREFIX);
			}

		}
		return null;
	}

	private Set<String> filterEnglishWordLabels(String iri, Set<String> synonyms) {
		Set<String> toReturn = new HashSet<String>(synonyms);
//		if (iriToExcludedEnglishWordLabels.containsKey(iri)) {
//			System.out.println("Excluding: " + iriToExcludedEnglishWordLabels.get(iri).toString());
//			toReturn.removeAll(iriToExcludedEnglishWordLabels.get(iri));
		toReturn.removeAll(toExcludedEnglishWordLabels);
//		}
		return toReturn;
	}

	private void logEnglishWords(String iri, Set<String> synonyms) {
		for (String syn : synonyms) {
			if (syn.length() > 2) {
				// we won't check the case sensitive synonyms b/c we want to allow those even if
				// the match an english word when lowercased
				if (!isCaseSensitive(syn)) {
					if (englishWords.contains(syn.toLowerCase())) {
						try {
							tmpWriter.write(String.format("%s\t%s\n", iri.toString(), syn));
						} catch (IOException e) {
							e.printStackTrace();
							System.exit(-1);
						}
					}
				}
			}
		}
	}

	/**
	 * This manual list was formed prior to the comprehensive check against English
	 * words
	 * 
	 * @param iri
	 * @param syns
	 * @return
	 */
	protected static Set<String> filterSpecificSynonyms(String iri, Set<String> syns) {

		Map<String, Set<String>> map = new HashMap<String, Set<String>>();

		map.put(OBO_PURL + "PR_000005054", new HashSet<String>(Arrays.asList("MICE")));
		map.put(OBO_PURL + "PR_P19576", new HashSet<String>(Arrays.asList("malE")));
		map.put(OBO_PURL + "PR_000034898", new HashSet<String>(Arrays.asList("yeaR")));
		map.put(OBO_PURL + "PR_000002226", new HashSet<String>(Arrays.asList("see")));
		map.put(OBO_PURL + "PR_P33696", new HashSet<String>(Arrays.asList("exoN")));
		map.put(OBO_PURL + "PR_P52558", new HashSet<String>(Arrays.asList("purE")));
		map.put(OBO_PURL + "PR_000023162", new HashSet<String>(Arrays.asList("manY")));
		map.put(OBO_PURL + "PR_000022679", new HashSet<String>(Arrays.asList("folD")));
		map.put(OBO_PURL + "PR_000023552", new HashSet<String>(Arrays.asList("pinE", "pin")));
		map.put(OBO_PURL + "PR_000023270", new HashSet<String>(Arrays.asList("modE")));
		map.put(OBO_PURL + "PR_Q8N1N2", new HashSet<String>(Arrays.asList("Full")));
		map.put(OBO_PURL + "PR_000034142", new HashSet<String>(Arrays.asList("casE")));
		map.put(OBO_PURL + "PR_P02301", new HashSet<String>(Arrays.asList("embryonic")));
		map.put(OBO_PURL + "PR_000023165", new HashSet<String>(Arrays.asList("map")));
		map.put(OBO_PURL + "PR_P19994", new HashSet<String>(Arrays.asList("map")));
		map.put(OBO_PURL + "PR_P0A078", new HashSet<String>(Arrays.asList("map")));
		map.put(OBO_PURL + "PR_000023786", new HashSet<String>(Arrays.asList("rna")));
		map.put(OBO_PURL + "PR_Q99856", new HashSet<String>(Arrays.asList("bright")));
		map.put(OBO_PURL + "PR_Q62431", new HashSet<String>(Arrays.asList("bright")));
		map.put(OBO_PURL + "PR_000008536", new HashSet<String>(Arrays.asList("Hrs")));
		map.put(OBO_PURL + "PR_Q9VAH4", new HashSet<String>(Arrays.asList("fig")));
		map.put(OBO_PURL + "PR_000016181", new HashSet<String>(Arrays.asList("Out")));
		map.put(OBO_PURL + "PR_000014717", new HashSet<String>(Arrays.asList("SET")));
		map.put(OBO_PURL + "PR_Q9VVR1", new HashSet<String>(Arrays.asList("not")));
		map.put(OBO_PURL + "PR_000029532", new HashSet<String>(Arrays.asList("embryonic")));
		map.put(OBO_PURL + "PR_000023516", new HashSet<String>(Arrays.asList("act")));
		map.put(OBO_PURL + "PR_000009667", new HashSet<String>(Arrays.asList("LARGE")));
		map.put(OBO_PURL + "PR_000002222", new HashSet<String>(Arrays.asList("LIGHT")));
		map.put(OBO_PURL + "PR_Q8INV7", new HashSet<String>(Arrays.asList("Victoria")));
		map.put(OBO_PURL + "PR_Q9CQS6", new HashSet<String>(Arrays.asList("Blot")));
		map.put(OBO_PURL + "PR_Q29TV8", new HashSet<String>(Arrays.asList("Blot")));
		map.put(OBO_PURL + "PR_Q62813", new HashSet<String>(Arrays.asList("Lamp")));
		map.put(OBO_PURL + "PR_000001067", new HashSet<String>(Arrays.asList("Alpha")));
		map.put(OBO_PURL + "PR_Q07342", new HashSet<String>(Arrays.asList("axial")));
		map.put(OBO_PURL + "PR_Q9V853", new HashSet<String>(Arrays.asList("lack")));
		map.put(OBO_PURL + "PR_Q9WVF7", new HashSet<String>(Arrays.asList("Pole")));
		map.put(OBO_PURL + "PR_Q54RD4", new HashSet<String>(Arrays.asList("pole")));
		map.put(OBO_PURL + "PR_Q7KRY6", new HashSet<String>(Arrays.asList("ball")));
		map.put(OBO_PURL + "PR_Q7TSD4", new HashSet<String>(Arrays.asList("Spatial")));
		map.put(OBO_PURL + "PR_000016877", new HashSet<String>(Arrays.asList("albino")));
		map.put(OBO_PURL + "PR_000001875", new HashSet<String>(Arrays.asList("diabetes")));
		map.put(OBO_PURL + "PR_000003944", new HashSet<String>(Arrays.asList("not")));
		map.put(OBO_PURL + "PR_000016001", new HashSet<String>(Arrays.asList("Low")));
		map.put(OBO_PURL + "PR_000009758", new HashSet<String>(Arrays.asList("obese", "obesity factor")));
		map.put(OBO_PURL + "PR_000013884", new HashSet<String>(Arrays.asList("Age")));
		map.put(OBO_PURL + "PR_000007972", new HashSet<String>(Arrays.asList("little")));
		map.put(OBO_PURL + "PR_000004386", new HashSet<String>(Arrays.asList("fold")));
		map.put(OBO_PURL + "PR_000005804", new HashSet<String>(Arrays.asList("fat")));
		map.put(OBO_PURL + "PR_000008215", new HashSet<String>(Arrays.asList("olfactory")));
		map.put(OBO_PURL + "PR_000011996", new HashSet<String>(Arrays.asList("olfactory receptor")));
		map.put(OBO_PURL + "PR_Q2PRA9", new HashSet<String>(Arrays.asList("olfactory receptor")));
		map.put(OBO_PURL + "PR_000011863", new HashSet<String>(Arrays.asList("odorant receptor")));
		map.put(OBO_PURL + "PR_Q2PRK7", new HashSet<String>(Arrays.asList("odorant receptor")));
		map.put(OBO_PURL + "PR_000005419", new HashSet<String>(Arrays.asList("predicted protein")));
		map.put(OBO_PURL + "PR_Q9UQG0", new HashSet<String>(Arrays.asList("polymerase")));
		map.put(OBO_PURL + "PR_000001308", new HashSet<String>(Arrays.asList("BLAST")));

		Set<String> updatedSyns = new HashSet<String>(syns);

		if (map.containsKey(iri)) {
			updatedSyns.removeAll(map.get(iri));
		}

		return updatedSyns;
	}

}
