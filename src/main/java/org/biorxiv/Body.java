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

public class Body implements Serializable {

    protected List<Section> sections;
    private final List<Object> contentList = new ArrayList<>();

    public List<Section> getSections() {
        if (sections == null) {
            return Collections.emptyList();
        }
        return sections;
    }

    public void setSections(List<Section> sections) {
        this.sections = sections;
    }

    public List<Object> getContentList() {
        return contentList;
    }

    public void addContent(Object content) {
        contentList.add(content);
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (Object contentObject : contentList) {
            builder.append(contentObject.toString());
            if (!contentObject.toString().endsWith("\n")) {
                builder.append('\n');
            }
        }
        return builder.toString();
    }

    @Override
    public boolean equals(@Nullable Object other) {
        return other instanceof Body &&
                this.getContentList().equals(((Body) other).getContentList()) &&
                this.getSections().equals(((Body) other).getSections());
    }

    public static Body parse(Node node, XPath xPath) {
        Body theBody = new Body();
        try {
            List<Section> sections = new ArrayList<>();
            NodeList elementsNodeList = (NodeList) xPath.compile("p|sec|fig").evaluate(node, XPathConstants.NODESET);
            for (int i = 0; i < elementsNodeList.getLength(); i++) {
                Node elementNode = elementsNodeList.item(i);
                switch (elementNode.getNodeName()) {
                    case "p":
                        theBody.addContent(Paragraph.parse(elementNode, xPath));
                        break;
                    case "sec":
                        Section section = Section.parse(elementNode, xPath);
                        sections.add(section);
                        section.getContentList().forEach(theBody::addContent);
                        break;
                    case "fig":
                        Fig figure = Fig.parse(elementNode, xPath);
                        theBody.addContent(figure);
                        break;
                }
            }

            theBody.setSections(sections);
        } catch (XPathExpressionException ex) {
            System.err.println("Could not compile XPath expression " + ex.getLocalizedMessage());
        }
        return theBody;
    }
}