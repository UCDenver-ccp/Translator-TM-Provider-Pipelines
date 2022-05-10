package edu.cuanschutz.ccp.tm_provider.etl.fn;

import edu.cuanschutz.ccp.tm_provider.etl.EtlFailureData;
import edu.cuanschutz.ccp.tm_provider.etl.ProcessingStatus;
import edu.cuanschutz.ccp.tm_provider.etl.util.*;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.bionlp.BioNLPDocumentWriter;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;
import lombok.Getter;
import lombok.Setter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.*;
import org.biorxiv.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static edu.cuanschutz.ccp.tm_provider.etl.PipelineMain.chunkContent;

public class JatsArticleToDocumentFn extends DoFn<Article, KV<String, List<String>>> {

    private final com.google.cloud.Timestamp timestamp;
    private final PCollectionView<Set<String>> currentDocIDs;
    private final Map<String, DocumentCriteria> outputCriteria;

    // 2155 is the max year value type in MySQL
    public static final String DEFAULT_PUB_YEAR = "2155";
    public static final String UNKNOWN_PUBLICATION_TYPE = "Unknown";
    public static TupleTag<KV<String, List<String>>> sectionAnnotationsTag = new TupleTag<KV<String, List<String>>>() {};
    public static TupleTag<EtlFailureData> etlFailureTag = new TupleTag<EtlFailureData>() {};
    public static TupleTag<ProcessingStatus> processingStatusTag = new TupleTag<ProcessingStatus>() {};

    public JatsArticleToDocumentFn(com.google.cloud.Timestamp timestamp, Map<String, DocumentCriteria> outputDocumentCriteria,
                                   PCollectionView<Set<String>> currentDocIDs) {
        this.timestamp = timestamp;
        this.outputCriteria = outputDocumentCriteria;
        this.currentDocIDs = currentDocIDs;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        Article biorxivArticle = context.element();
        if(biorxivArticle == null) {
            return;
        }
        DocumentCriteria sectionsDocumentCriteria = outputCriteria.get("sections");
        Set<String> alreadyStoredDocIds = context.sideInput(currentDocIDs);
        TextDocumentWithMetadata document = buildDocument(biorxivArticle);
        if(document == null || alreadyStoredDocIds == null) {
            return;
        }
        if (sectionsDocumentCriteria != null) {
            try {
                outputDocument(context, document, sectionAnnotationsTag);
            } catch (Throwable t) {
                EtlFailureData failureData = new EtlFailureData(sectionsDocumentCriteria,
                        "Potential error outputting section annotations",
                        document.getSourceid(), t, timestamp);
                context.output(etlFailureTag, failureData);
            }
        }
    }

    public static TextDocumentWithMetadata buildDocument(Article article) {
        ArticleMeta articleMeta = article.getFront().getArticleMeta();
        Body body = article.getBody();

        //region Get data from meta
        List<ArticleID> ids = articleMeta.getArticleIDs();
        if(ids == null || ids.size() == 0) {
            return null;
        }
        String doi = null;
        for (ArticleID id : ids) {
            if (id.getPubIdType() != null && id.getPubIdType().equals("doi")) {
                doi = id.getValue();
            }
        }
        if (doi == null || doi.equals("")) {
            return null;
        }
        String title = articleMeta.getTitleGroup().getArticleTitle();
        String yearString = getYearPublished(articleMeta);
        String abstractText = getAbstractText(articleMeta);
        //endregion

        //region Get data from body
        StringBuilder documentTextBuilder = new StringBuilder();
        documentTextBuilder.append(title);
        if (documentTextBuilder.charAt(documentTextBuilder.length() - 1) != '\n') {
            documentTextBuilder.append("\n\n");
        }
        documentTextBuilder.append(abstractText);
        if (body != null) {
            if (documentTextBuilder.charAt(documentTextBuilder.length() - 1) != '\n') {
                documentTextBuilder.append("\n\n");
            }
            documentTextBuilder.append(body);
        }
        String documentText = documentTextBuilder.toString();
        //endregion

        //region Build document
        int startIndex = 0;
        TextDocumentWithMetadata doc = new TextDocumentWithMetadata(doi, "BioRxiv", documentText);

        TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults(doi);

        doc.addAnnotation(factory.createAnnotation(0, title.length(), title, "title"));
        startIndex += title.length();

        if (!abstractText.trim().isEmpty()) {
            int start = documentText.indexOf(abstractText, startIndex);
            int end = start + abstractText.length();
            startIndex += abstractText.length();
            doc.addAnnotation(factory.createAnnotation(start, end, abstractText, "abstract"));
        }

        if (body != null && !body.toString().trim().isEmpty()) {
            int startIndexCopy = startIndex;
            // Iterate the top level sections (this assumes they are in document order)
            for (Section section : body.getSections()) {
                String textContent = section.getTextContents();
                int start = documentText.indexOf(textContent, startIndex);
                int end = start + textContent.length();
                if (start < 0) {
                    start = documentText.indexOf(section.toString(), startIndex);
                    end = start + section.toString().length();
                    startIndex += section.toString().length();
                } else {
                    startIndex += textContent.length();
                }
                doc.addAnnotation(factory.createAnnotation(start, end, textContent, "section"));
            }
            startIndex = startIndexCopy; // Reset the start index to search for paragraph-level annotations from the top
            // Iterate the paragraph-level annotations (this assumes they are in document order)
            for (Object contentObject : body.getContentList()) {
                String textContent = contentObject.toString();
                String annotationCategory = "paragraph";
                int start = documentText.indexOf(textContent, startIndex);
                int end = start + textContent.length();
                startIndex += textContent.length();
                if (contentObject instanceof String) {
                    annotationCategory = "section-heading";
                } else if (contentObject instanceof Fig) {
                    annotationCategory = "figure-caption";
                }
                doc.addAnnotation(factory.createAnnotation(start, end, textContent, annotationCategory));
            }
        }

        doc.setYearPublished(yearString);
        //endregion

        return doc;
    }

    private static String getAbstractText(ArticleMeta meta) {
        List<Abstract> abstractList = meta.getAbstracts();
        if(abstractList == null || abstractList.size() == 0) {
            return "";
        }
        StringBuilder abstractStringBuilder = new StringBuilder();
        // Depending on how it works with multiple Abstract sections this may need a newline separator.
        for(Abstract abs : abstractList) {
            abstractStringBuilder.append(abs);
        }
        return abstractStringBuilder.toString();
    }

    private static String getYearPublished(ArticleMeta meta) {
        for(PubDate pubDate : meta.getPubDate()) {
            if (pubDate.getPubType() != null && pubDate.getPubType().equals("epub")) {
                return pubDate.getYear();
            }
        }
        return DEFAULT_PUB_YEAR;
    }

    public static void outputDocument(ProcessContext context, TextDocumentWithMetadata doc, TupleTag<KV<String, List<String>>> tag)
            throws IOException {
        BioNLPDocumentWriter writer = new BioNLPDocumentWriter();
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        writer.serialize(doc, stream, CharacterEncoding.UTF_8);
        String serializedAnnotations = stream.toString(CharacterEncoding.UTF_8.getCharacterSetName());
        List<String> chunkedAnnotations = chunkContent(serializedAnnotations);
        context.output(tag, KV.of(doc.getSourceid(), chunkedAnnotations));
        ProcessingStatus status = new ProcessingStatus(doc.getSourceid());
        status.setYearPublished(doc.getYearPublished() == null ? DEFAULT_PUB_YEAR : doc.getYearPublished());
        if (doc.getPublicationTypes() != null && !doc.getPublicationTypes().isEmpty()) {
            for (String pt : doc.getPublicationTypes()) {
                status.addPublicationType(pt);
            }
        } else {
            status.addPublicationType(UNKNOWN_PUBLICATION_TYPE);
        }
        status.enableFlag(ProcessingStatusFlag.SECTIONS_DONE);

        context.output(processingStatusTag, status);
    }

    static class TextDocumentWithMetadata extends TextDocument {
        @Setter
        @Getter
        private String yearPublished;

        @Getter
        private final List<String> publicationTypes = new ArrayList<>();

        public TextDocumentWithMetadata(String sourceid, String sourcedb, String text) {
            super(sourceid, sourcedb, text);
        }

        public void addPublicationType(String pubType) {
            publicationTypes.add(pubType);
        }
    }
}
