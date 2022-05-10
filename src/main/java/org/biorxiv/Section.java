package org.biorxiv;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nullable;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Section implements Serializable {

    protected String title;
    private final List<Object> contentList = new ArrayList<>();

    public String getTitle() {
        if (title == null) {
            title = "";
        }
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<Object> getContentList() {
        return contentList;
    }

    public void addContent(Object content) {
        this.contentList.add(content);
    }

    // The default String representation of a Section is the concatenation of all text nodes contained in it and all
    // sub-sections, in top to bottom order and flattening any nested elements.
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (Object contentObject : contentList) {
            builder.append(contentObject);
            if (!contentObject.toString().endsWith("\n")) {
                builder.append('\n');
            }
        }
        return builder.toString();
    }

    public String getTextContents() {
        StringBuilder builder = new StringBuilder();
        for (Object contentObject : contentList) {
            if (contentObject instanceof Section) {
                builder.append(((Section) contentObject).getTextContents());
                if (!((Section) contentObject).getTextContents().endsWith("\n")) {
                    builder.append("\n");
                }
            } else if (!(contentObject instanceof String)) {
                builder.append(contentObject);
                if (!contentObject.toString().endsWith("\n")) {
                    builder.append("\n");
                }
            }
        }
        return builder.toString();
    }

    public static Section parse(Node sectionNode, XPath xPath) {
        Section newSection = new Section();
        try {
            Node titleNode = (Node) xPath.compile("title").evaluate(sectionNode, XPathConstants.NODE);
            if (titleNode != null) {
                newSection.setTitle(titleNode.getTextContent());
                newSection.addContent(titleNode.getTextContent());
            }
            NodeList elementsNodeList = (NodeList) xPath.compile("p|fig|sec").evaluate(sectionNode, XPathConstants.NODESET);
            for (int i = 0; i < elementsNodeList.getLength(); i++) {
                Node elementNode = elementsNodeList.item(i);
                switch (elementNode.getNodeName()) {
                    case "p":
                        Paragraph paragraph = Paragraph.parse(elementNode, xPath);
                        newSection.addContent(paragraph);
                        break;
                    case "sec":
                        Section section = Section.parse(elementNode, xPath);
                        section.getContentList().forEach(newSection::addContent);
                        break;
                    case "fig":
                        Fig figure = Fig.parse(elementNode, xPath);
                        newSection.addContent(figure);
                }
            }
        } catch (XPathExpressionException ex) {
            System.err.println("Could not compile XPath expression " + ex.getLocalizedMessage());
        }
        return newSection;
    }
    @Override
    public boolean equals(@Nullable Object other) {
        return other instanceof Section &&
                this.getTitle().equals(((Section) other).getTitle()) &&
                this.getContentList().equals(((Section) other).getContentList());
    }
}
