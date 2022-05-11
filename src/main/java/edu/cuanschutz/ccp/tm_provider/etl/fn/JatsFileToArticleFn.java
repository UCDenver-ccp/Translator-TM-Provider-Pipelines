package edu.cuanschutz.ccp.tm_provider.etl.fn;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.biorxiv.Article;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import static edu.cuanschutz.ccp.tm_provider.etl.BiorxivXmlToTextPipeline.LOGGER;

public class JatsFileToArticleFn extends DoFn<FileIO.ReadableFile, Article> {

    @ProcessElement
    public void processElement(ProcessContext c) {
        DocumentBuilder builder;
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            builder = factory.newDocumentBuilder();
        } catch (ParserConfigurationException pce) {
            LOGGER.log(Level.WARNING, pce.getMessage());
            return;
        }
        FileIO.ReadableFile file = c.element();
        List<Article> articleList = new ArrayList<>();
        if (file == null) {
            return;
        }
        try {
            String fileContents = file.readFullyAsUTF8String();
            InputSource source = new InputSource(new StringReader(fileContents));
            builder.setEntityResolver((publicId, systemId) -> {
                String terminalPart = systemId.substring(systemId.lastIndexOf('/'));
                if (systemId.contains("/iso8879/") || systemId.contains("/iso9573-13/") || systemId.contains("/mathml/") ||
                        systemId.contains("/Smallsamples/") || systemId.contains("/xmlchars/")) {
                    int lastIndex = systemId.lastIndexOf('/');
                    int secondLastIndex = systemId.substring(0, lastIndex - 1).lastIndexOf('/');
                    terminalPart = systemId.substring(secondLastIndex);
                }
                ClassLoader loader = Thread.currentThread().getContextClassLoader();
                InputStream stream2 = loader.getResourceAsStream("biorxiv" + terminalPart);
                InputSource source2 = new InputSource(stream2);
                source2.setPublicId(publicId);
                return source2;
            });
            Document document = builder.parse(source);
            XPath xpath = XPathFactory.newInstance().newXPath(); // Neither XPathFactory nor XPath are thread-safe.
            NodeList articles = Article.getArticles(document, xpath);
            if (articles != null && articles.getLength() > 0) {
                for (int i = 0; i < articles.getLength(); i++) {
                    articleList.add(Article.parse(articles.item(i), xpath));
                }
            } else {
                try {
                    Node article = (Node) xpath.compile("/article").evaluate(document, XPathConstants.NODE);
                    articleList.add(Article.parse(article, xpath));
                } catch (XPathExpressionException ex) {
                    LOGGER.warning("Failed to compile xpath for article");
                }
            }
        } catch (Exception ex) {
            LOGGER.log(Level.WARNING, ex.getMessage());
            LOGGER.log(Level.FINE, "Exception when reading files", ex);
        }
        for (Article article : articleList) {
            c.output(article);
        }
    }
}
