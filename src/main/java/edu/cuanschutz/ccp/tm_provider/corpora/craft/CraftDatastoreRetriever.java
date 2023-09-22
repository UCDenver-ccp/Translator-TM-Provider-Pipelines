package edu.cuanschutz.ccp.tm_provider.corpora.craft;

import static edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants.DOCUMENT_PROPERTY_CONTENT;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StructuredQuery.CompositeFilter;
import com.google.cloud.datastore.StructuredQuery.Filter;
import com.google.cloud.datastore.StructuredQuery.PropertyFilter;
import com.google.cloud.datastore.Value;

import edu.cuanschutz.ccp.tm_provider.corpora.craft.ExcludeCraftNestedConcepts.Ont;
import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants;
import edu.cuanschutz.ccp.tm_provider.etl.util.DocumentType;
import edu.cuanschutz.ccp.tm_provider.etl.util.PipelineKey;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.file.FileReaderUtil;
import edu.ucdenver.ccp.common.file.FileUtil;
import edu.ucdenver.ccp.common.file.FileWriterUtil;
import edu.ucdenver.ccp.common.file.reader.Line;
import edu.ucdenver.ccp.common.file.reader.StreamLineIterator;
import edu.ucdenver.ccp.common.string.StringUtil;
import edu.ucdenver.ccp.datasource.fileparsers.obo.OntologyUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentReader;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentWriter;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;

/**
 * Facilitates the retrieval of all CRAFT documents from Cloud Datastore
 *
 */
public class CraftDatastoreRetriever {

	private static final CharacterEncoding ENCODING = CharacterEncoding.UTF_8;
	// Create an authorized Datastore service using Application Default Credentials.
	private final Datastore datastore = DatastoreOptions.getDefaultInstance().getService();

	public enum FilterByCrf {
		YES, NO
	}

	public enum Target {
		/**
		 * OGER indicates retrieve the raw OGER output
		 */
		OGER,
		/**
		 * PP = Post-processed
		 */
		PP,
		/**
		 * PP_CRF = Post-processed then filtered by CRF
		 */
		PP_CRF
	}

	/**
	 * Retrieve the CRAFT text documents from Datastore
	 * 
	 * @param outputDirectory
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public void getTextDocuments(File outputDirectory) throws FileNotFoundException, IOException {

		Filter craftCollectionFilter = PropertyFilter.eq(DatastoreConstants.DOCUMENT_PROPERTY_COLLECTIONS, "CRAFT");
		Filter textFilter = PropertyFilter.eq(DatastoreConstants.DOCUMENT_PROPERTY_TYPE, DocumentType.TEXT.name());

		CompositeFilter filter = CompositeFilter.and(craftCollectionFilter, textFilter);

		runDatastoreQuery(outputDirectory, filter);

	}

	/**
	 * Retrieve all OGER documents in the CRAFT collection - for each document, this
	 * should retrieve 3 documents corresponding to the case-sensitive,
	 * case-insensitive-min-norm, and case-insensitive-max-norm OGER runs
	 * 
	 * @param outputDirectory
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public void getOgerDocuments(File outputDirectory, String ogerPipelineVersion)
			throws FileNotFoundException, IOException {

		Filter craftCollectionFilter = PropertyFilter.eq(DatastoreConstants.DOCUMENT_PROPERTY_COLLECTIONS, "CRAFT");
		Filter ogerPipelineFilter = PropertyFilter.eq(DatastoreConstants.DOCUMENT_PROPERTY_PIPELINE,
				PipelineKey.OGER.name());
		Filter ogerPipelineVersionFilter = PropertyFilter.eq(DatastoreConstants.DOCUMENT_PROPERTY_PIPELINE_VERSION,
				ogerPipelineVersion);

		CompositeFilter filter = CompositeFilter.and(craftCollectionFilter, ogerPipelineFilter,
				ogerPipelineVersionFilter);

		runDatastoreQuery(outputDirectory, filter);

	}

	private void runDatastoreQuery(File outputDirectory, CompositeFilter filter)
			throws IOException, FileNotFoundException {
		EntityQuery query = Query.newEntityQueryBuilder().setKind(DatastoreConstants.DOCUMENT_KIND).setFilter(filter)
				.build();

		QueryResults<Entity> queryResults = datastore.run(query);
		while (queryResults.hasNext()) {
			Entity entity = queryResults.next();
			Map<String, Value<?>> properties = entity.getProperties();

			String docId = properties.get(DatastoreConstants.DOCUMENT_PROPERTY_ID).get().toString();
			docId = StringUtil.removePrefix(docId, "craft-");
			docId = StringUtil.removeLastCharacter(docId);
			String content = new String(entity.getBlob(DOCUMENT_PROPERTY_CONTENT).toByteArray());
			String docType = properties.get(DatastoreConstants.DOCUMENT_PROPERTY_TYPE).get().toString();

			File outputFile = new File(outputDirectory, String.format("%s.%s", docId, docType.toLowerCase()));

			try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(outputFile)) {
				writer.write(content);
			}

		}
	}

	/**
	 * Retrieve all post-processed concept documents in the CRAFT collection
	 * 
	 * @param outputDirectory
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public void getPostProcessedDocuments(File outputDirectory, Target target, String postProcessPipelineVersion)
			throws FileNotFoundException, IOException {

		FilterByCrf filterByCrf = (target == Target.PP_CRF) ? FilterByCrf.YES : FilterByCrf.NO;

		Filter craftCollectionFilter = PropertyFilter.eq(DatastoreConstants.DOCUMENT_PROPERTY_COLLECTIONS, "CRAFT");
		Filter pipelineFilter = PropertyFilter.eq(DatastoreConstants.DOCUMENT_PROPERTY_PIPELINE,
				PipelineKey.CONCEPT_POST_PROCESS.name());
		Filter crfFilter = null;
		if (filterByCrf == FilterByCrf.YES) {
			crfFilter = PropertyFilter.eq(DatastoreConstants.DOCUMENT_PROPERTY_TYPE, DocumentType.CONCEPT_ALL.name());
		} else {
			crfFilter = PropertyFilter.eq(DatastoreConstants.DOCUMENT_PROPERTY_TYPE,
					DocumentType.CONCEPT_ALL_UNFILTERED.name());
		}

		Filter versionFilter = PropertyFilter.eq(DatastoreConstants.DOCUMENT_PROPERTY_PIPELINE_VERSION,
				postProcessPipelineVersion);

		CompositeFilter filter = CompositeFilter.and(craftCollectionFilter, pipelineFilter, crfFilter, versionFilter);

		runDatastoreQuery(outputDirectory, filter);

//		EntityQuery query = Query.newEntityQueryBuilder().setKind(DatastoreConstants.DOCUMENT_KIND).setFilter(filter)
//				.build();
//
//		QueryResults<Entity> queryResults = datastore.run(query);
//		while (queryResults.hasNext()) {
//			Entity entity = queryResults.next();
//			Map<String, Value<?>> properties = entity.getProperties();
//
//			String docId = properties.get(DatastoreConstants.DOCUMENT_PROPERTY_ID).get().toString();
//			docId = StringUtil.removePrefix(docId, "craft-");
//			docId = StringUtil.removeLastCharacter(docId);
//			String content = new String(entity.getBlob(DOCUMENT_PROPERTY_CONTENT).toByteArray());
//			String docType = properties.get(DatastoreConstants.DOCUMENT_PROPERTY_TYPE).get().toString();
//
//			File outputFile = new File(outputDirectory, String.format("%s.%s", docId, docType.toLowerCase()));
//
//			try (BufferedWriter writer = FileWriterUtil.initBufferedWriter(outputFile)) {
//				writer.write(content);
//			}
//
//		}

	}

	/**
	 * Consume the files that were downloaded from Datastore, and create a file for
	 * each ontology that contains essentially the raw OGER output. Annotations are
	 * segregated by looking at their namespace. This may not be ideal as some
	 * namespaces are shared between ontologies, e.g., there are some GO protein
	 * complex classes in PR.
	 * 
	 * @param datastoreDir
	 * @param outputDir
	 * @throws IOException
	 */
	private static void createOgerFiles(File datastoreDir, // File go2namespaceFile,
			File outputDir, File craftBaseDir, Target target) throws IOException {
//		System.out.println("Loading go2ontmap...");
//		Map<String, Ont> goCurieToOntMap = loadMapFile(go2namespaceFile);

		Map<String, File> docIdToDirMap = createDocIdToTrainDevTestDirMap(outputDir, craftBaseDir);

		for (Iterator<File> fileIter = FileUtil.getFileIterator(datastoreDir, false, "text"); fileIter.hasNext();) {
			File txtFile = fileIter.next();

			String docId = txtFile.getName().split("\\.")[0];
			System.out.println("Processing: " + docId);
			File dir = docIdToDirMap.get(docId);

			List<File> annotFiles = new ArrayList<File>();

			switch (target) {
			case OGER:
				annotFiles.addAll(getOgerFiles(datastoreDir, docId));
				break;
			case PP:
				annotFiles.addAll(getPostProcFiles(datastoreDir, docId, target));
				break;
			case PP_CRF:
				annotFiles.addAll(getPostProcFiles(datastoreDir, docId, target));
				break;
			default:
				throw new IllegalArgumentException("should not be here");
			}

			Map<Ont, TextDocument> ontToDocMap = initOntToDocMap(docId, txtFile, target);
			for (File file : annotFiles) {
				populateOntToDocMap(docId, file, txtFile, ontToDocMap,
//						goCurieToOntMap, 
						target);
			}

			serializeBionlpFiles(docId, ontToDocMap, dir);

		}

	}

	/**
	 * @param datastoreDir
	 * @param docId
	 * @param target
	 * @return the post-processed annotation file for the specified docId
	 */
	private static Collection<? extends File> getPostProcFiles(File datastoreDir, String docId, Target target) {
		String suffix = (target == Target.PP_CRF) ? DocumentType.CONCEPT_ALL.name().toLowerCase()
				: DocumentType.CONCEPT_ALL_UNFILTERED.name().toLowerCase();

		File ppFile = new File(datastoreDir, String.format("%s.%s", docId, suffix));

		return Arrays.asList(ppFile);
	}

	/**
	 * @param datastoreDir
	 * @param docId
	 * @return the three OGER files associated with the given docId
	 */
	private static Collection<? extends File> getOgerFiles(File datastoreDir, String docId) {
		File csFile = new File(datastoreDir, String.format("%s.concept_cs", docId));
		File ciminFile = new File(datastoreDir, String.format("%s.concept_cimin", docId));
		File cimaxFile = new File(datastoreDir, String.format("%s.concept_cimax", docId));

		return Arrays.asList(csFile, ciminFile, cimaxFile);
	}

	/**
	 * Initialize a TextDocument for each possible ontology
	 * 
	 * @param docId
	 * @param txtFile
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private static Map<Ont, TextDocument> initOntToDocMap(String docId, File txtFile, Target target)
			throws FileNotFoundException, IOException {
		Map<Ont, TextDocument> ontToDocMap = new HashMap<Ont, TextDocument>();

		String docText = IOUtils.toString(new FileInputStream(txtFile), ENCODING.getCharacterSetName());
		for (Ont ont : Ont.values()) {
			if (target == Target.PP || target == Target.PP_CRF) {
				if (ont == Ont.DRUGBANK || ont == Ont.SNOMEDCT || ont == Ont.HP) {
					// the eval platform balks when there are non CRAFT ontology directories, so
					// when we download the PP files, we skip these ontologies. When we download the
					// OGER files, we won't skip so that we can view all annotations.
					continue;
				}
			}
			TextDocument td = new TextDocument(docId, "craft", docText);
			td.setAnnotations(new ArrayList<TextAnnotation>());
			ontToDocMap.put(ont, td);
		}

		return ontToDocMap;

	}

	/**
	 * Create a mapping from the CRAFT document ID to the train/dev/test directory
	 * where the data should be stored
	 * 
	 * @param outputDir
	 * @param craftBaseDir
	 * @return
	 * @throws IOException
	 */
	private static Map<String, File> createDocIdToTrainDevTestDirMap(File outputDir, File craftBaseDir)
			throws IOException {
		File trainDir = new File(outputDir, "train");
		File devDir = new File(outputDir, "dev");
		File testDir = new File(outputDir, "test");

		Set<String> trainIds = loadCraftIds("train", craftBaseDir);
		Set<String> devIds = loadCraftIds("dev", craftBaseDir);
		Set<String> testIds = loadCraftIds("test", craftBaseDir);

		Map<String, File> docIdToDirMap = new HashMap<String, File>();
		for (String docId : trainIds) {
			docIdToDirMap.put(docId, trainDir);
		}
		for (String docId : devIds) {
			docIdToDirMap.put(docId, devDir);
		}
		for (String docId : testIds) {
			docIdToDirMap.put(docId, testDir);
		}
		return docIdToDirMap;
	}

	/**
	 * Loads id files from the train/dev/test sets as specified in the CRAFT
	 * distribution
	 * 
	 * @param setKey       - train, dev, or test
	 * @param craftBaseDir
	 * @return
	 * @throws IOException
	 */
	private static Set<String> loadCraftIds(String setKey, File craftBaseDir) throws IOException {
		File idFile = new File(craftBaseDir, String.format("articles/ids/craft-ids-%s.txt", setKey));
		return new HashSet<String>(FileReaderUtil.loadLinesFromFile(idFile, ENCODING));
	}

	/**
	 * output bionlp files forthe concept annotations, split into directories based
	 * on ontology as well as train/dev/test splits as defined in the CRAFT corpus
	 * 
	 * @param docId
	 * @param ontToDocMap
	 * @param outputDir
	 * @param craftBaseDir
	 * @throws IOException
	 */
	private static void serializeBionlpFiles(String docId, Map<Ont, TextDocument> ontToDocMap, File outputDir)
			throws IOException {
		BioNLPDocumentWriter bionlpWriter = new BioNLPDocumentWriter();

		for (Entry<Ont, TextDocument> entry : ontToDocMap.entrySet()) {
			Ont ont = entry.getKey();
			TextDocument td = entry.getValue();

			File outputFile = new File(outputDir, String.format("%s/%s.bionlp", ont.name().toLowerCase(), docId));

			if (ont == Ont.MONDO) {
				outputFile = new File(outputDir,
						String.format("%s/%s.bionlp", "MONDO_WITHOUT_GENOTYPE_ANNOTATIONS".toLowerCase(), docId));
			}

			File dir = outputFile.getParentFile();
			if (!dir.exists()) {
				dir.mkdirs();
			}

			bionlpWriter.serialize(td, outputFile, ENCODING);

//			// there are two options for MONDO evaluation, so we'll add the second one here
//			// - at least for now.
//			if (ont == Ont.MONDO) {
//				outputFile = new File(outputDir,
//						String.format("%s/%s.bionlp", "MONDO_WITHOUT_GENOTYPE_ANNOTATIONS".toLowerCase(), docId));
//				dir = outputFile.getParentFile();
//				if (!dir.exists()) {
//					dir.mkdirs();
//				}
//				bionlpWriter.serialize(td, outputFile, ENCODING);
//			}
		}
	}

	private static void populateOntToDocMap(String docId, File annotFile, File txtFile,
			Map<Ont, TextDocument> ontToDocMap // , Map<String, Ont> go2ontMap
			, Target target) throws IOException {
		BioNLPDocumentReader bionlpReader = new BioNLPDocumentReader();

		TextDocument td = bionlpReader.readDocument(docId, "craft", annotFile, txtFile, ENCODING);

		for (TextAnnotation annot : td.getAnnotations()) {
			String id = annot.getClassMention().getMentionName();
			String prefix = id.split(":")[0];
			Ont ont = null;

			if (prefix.contains("GO_")) {
				if (prefix.equals("GO_BP")) {
					ont = Ont.GO_BP;
				} else if (prefix.equals("GO_CC")) {
					ont = Ont.GO_CC;
				} else if (prefix.equals("GO_MF")) {
					ont = Ont.GO_MF;
				}
				id = String.format("GO:%s", id.split(":")[1]);
				annot.getClassMention().setMentionName(id);
//				ont = go2ontMap.get(id);
			} else {
				try {
					ont = Ont.valueOf(prefix);
				} catch (IllegalArgumentException e) {
					ont = null;
				}
			}

			if (target == Target.PP || target == Target.PP_CRF) {
				if (ont == Ont.DRUGBANK || ont == Ont.SNOMEDCT || ont == Ont.HP) {
					// the eval platform balks when there are non CRAFT ontology directories, so
					// when we download the PP files, we skip these ontologies. When we download the
					// OGER files, we won't skip so that we can view all annotations.
					continue;
				}
			}

			if (ont == null & !prefix.equals("TMKPUTIL")) {
				throw new IllegalArgumentException("Null Ont for " + id);
			} else if (!prefix.equals("TMKPUTIL")) {
				if (ontToDocMap.containsKey(ont)) {
					ontToDocMap.get(ont).addAnnotation(annot);
				} else {
					TextDocument ontTd = new TextDocument(docId, "craft", td.getText());
					ontTd.addAnnotation(annot);
					ontToDocMap.put(ont, ontTd);
				}
			}
		}

	}

	/**
	 * loads a mapping from GO CURIE to Ont from a file derived from go.owl
	 * 
	 * @param go2namespaceFile
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private static Map<String, Ont> loadMapFile(File go2namespaceFile) throws FileNotFoundException, IOException {
		Map<String, Ont> map = new HashMap<String, Ont>();
		for (StreamLineIterator lineIter = new StreamLineIterator(
				new GZIPInputStream(new FileInputStream(go2namespaceFile)), ENCODING, null); lineIter.hasNext();) {
			Line line = lineIter.next();

			String[] cols = line.getText().split("\\t");
			String id = cols[0];
			String namespace = cols[1];

			switch (namespace) {
			case "biological_process":
				map.put(id, Ont.GO_BP);
				break;
			case "cellular_component":
				map.put(id, Ont.GO_CC);
				break;
			case "molecular_function":
				map.put(id, Ont.GO_MF);
				break;
			default:
				throw new IllegalArgumentException(String.format("Unknown namespace for %s = %s", id, namespace));
			}
		}
		return map;
	}

	private static void createGo2NamespaceFile(File goOwlFile, File outputFile)
			throws OWLOntologyCreationException, FileNotFoundException, IOException {
		OntologyUtil ontUtil = new OntologyUtil(goOwlFile);

		try (BufferedWriter writer = FileWriterUtil
				.initBufferedWriter(new GZIPOutputStream(new FileOutputStream(outputFile)), ENCODING)) {
			for (Iterator<OWLClass> classIterator = ontUtil.getClassIterator(); classIterator.hasNext();) {
				OWLClass cls = classIterator.next();
				String namespace = ontUtil.getNamespace(cls);
				if (namespace != null) {
					String iri = cls.getIRI().toString();
					int lastSlash = iri.lastIndexOf("/");
					String id = iri.substring(lastSlash + 1);
					id = id.replace("_", ":");
					namespace = namespace.replace("\"", "");
					writer.write(String.format("%s\t%s\n", id, namespace));
				}
			}
		}
	}

	public static void main(String[] args) {

		String version = args[0];
		Target target = Target.valueOf(args[1]);
		String pipelineVersion = args[2];

		File intermediateOutputDir = new File(String.format(
				"/Users/bill/projects/ncats-translator/concept-recognition/july2023/craft-output-files/%s/%s",
				target.name().toLowerCase(), version));

		File finalOutputDir = new File(
				String.format("/Users/bill/projects/ncats-translator/concept-recognition/july2023/%s/%s",
						target.name().toLowerCase(), version));
		File go2namespaceFile = new File(
				"/Users/bill/projects/ncats-translator/concept-recognition/july2023/go2namespace.tsv.gz");
		File ontBase = new File("/Users/bill/projects/ncats-translator/ontology-resources/ontologies/20230716");
		File goOwlFile = new File(ontBase, "go.owl");
		File craftBaseDir = new File("/Users/bill/projects/craft-shared-task/exclude-nested-concepts/craft.git");

//		try {
//			createGo2NamespaceFile(goOwlFile, go2namespaceFile);
//		} catch (IOException | OWLOntologyCreationException e) {
//			e.printStackTrace();
//		}

		intermediateOutputDir.mkdirs();
		finalOutputDir.mkdirs();

		try {
			CraftDatastoreRetriever cdr = new CraftDatastoreRetriever();
			cdr.getTextDocuments(intermediateOutputDir);
			switch (target) {
			case OGER:
				cdr.getOgerDocuments(intermediateOutputDir, pipelineVersion);
				break;
			case PP:
				cdr.getPostProcessedDocuments(intermediateOutputDir, target, pipelineVersion);
				break;
			case PP_CRF:
				cdr.getPostProcessedDocuments(intermediateOutputDir, target, pipelineVersion);
				break;
			default:
				throw new IllegalArgumentException("should not be here");
			}
			createOgerFiles(intermediateOutputDir, // go2namespaceFile,
					finalOutputDir, craftBaseDir, target);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
