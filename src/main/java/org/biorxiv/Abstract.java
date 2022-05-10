package org.biorxiv;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nullable;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Abstract implements Serializable {

    protected String title;
    protected List<String> paragraphs;
    protected List<Section> sections;
    protected List<Fig> figures;

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<String> getParagraphs() {
        if (paragraphs == null) {
            return Collections.emptyList();
        }
        return paragraphs;
    }

    public void setParagraphs(List<String> paragraphs) {
        this.paragraphs = paragraphs;
    }

    public List<Section> getSections() {
        if (sections == null) {
            return Collections.emptyList();
        }
        return sections;
    }

    public void setSections(List<Section> sections) {
        this.sections = sections;
    }

    public List<Fig> getFigures() {
        return figures;
    }

    public void setFigures(List<Fig> figures) {
        this.figures = figures;
    }

    //Naive first attempt at turning the paragraphs and sections into a single full text. Probably going to be out of order.
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (String paragraph : this.getParagraphs()) {
            builder.append(paragraph);
        }
        for (Section section : this.getSections()) {
            builder.append(section.toString());
        }
        for (Fig figure : this.getFigures()) {
            builder.append(figure.getCaption());
        }
        if (builder.charAt(builder.length() - 1) != '\n') {
            builder.append('\n');
        }
        return builder.toString();
    }

    @Override
    public boolean equals(@Nullable Object other) {
        return other instanceof Abstract &&
                this.getTitle().equals(((Abstract) other).getTitle()) &&
                this.getSections().equals(((Abstract) other).getSections()) &&
                this.getParagraphs().equals(((Abstract) other).getParagraphs()) &&
                this.getFigures().equals(((Abstract) other).getFigures());
    }

    public static Abstract parse(Node node, XPath xPath) {
        Abstract theAbstract = new Abstract();
        try {
            Node titleNode = (Node) xPath.compile("title").evaluate(node, XPathConstants.NODE);
            if (titleNode != null) {
                theAbstract.setTitle(titleNode.getTextContent());
            }
            NodeList paragraphNodeList = (NodeList) xPath.compile("p|list/list-item/p").evaluate(node, XPathConstants.NODESET);
            List<String> paragraphs = new ArrayList<>();
            for (int i = 0; i < paragraphNodeList.getLength(); i++) {
                Node paragraphNode = paragraphNodeList.item(i);
                paragraphs.add(paragraphNode.getTextContent());
            }
            theAbstract.setParagraphs(paragraphs);
            NodeList sectionNodeList = (NodeList) xPath.compile("sec").evaluate(node, XPathConstants.NODESET);
            List<Section> sections = new ArrayList<>();
            for (int i = 0; i < sectionNodeList.getLength(); i++) {
                sections.add(Section.parse(sectionNodeList.item(i), xPath));
            }
            theAbstract.setSections(sections);
            NodeList figureNodeList = (NodeList) xPath.compile("fig").evaluate(node, XPathConstants.NODESET);
            List<Fig> figures = new ArrayList<>();
            for (int i = 0; i < figureNodeList.getLength(); i++) {
                figures.add(Fig.parse(figureNodeList.item(i), xPath));
            }
            theAbstract.setFigures(figures);
        } catch (XPathExpressionException ex) {
            System.err.println("Could not compile XPath expression " + ex.getLocalizedMessage());
        }
        return theAbstract;
    }
}
