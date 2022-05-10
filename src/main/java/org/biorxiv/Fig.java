package org.biorxiv;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nullable;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import java.io.Serializable;

public class Fig implements Serializable {

    protected String caption;

    public String getCaption() {
        return caption;
    }

    public void setCaption(String caption) {
        this.caption = caption;
    }

    @Override
    public boolean equals(@Nullable Object other) {
        return other instanceof Fig &&
                this.getCaption().equals(((Fig) other).getCaption());
    }

    @Override
    public String toString() {
        return this.caption;
    }

    public static Fig parse(Node node, XPath xPath) {
        Fig figure = new Fig();
        try {
            NodeList paragraphNodes = (NodeList) xPath.compile("p|caption/p").evaluate(node, XPathConstants.NODESET);
            StringBuilder captionBuilder = new StringBuilder();
            for (int i = 0; i < paragraphNodes.getLength(); i++) {
                Node paragraphNode = paragraphNodes.item(i);
                captionBuilder.append(paragraphNode.getTextContent());
            }
            figure.setCaption(captionBuilder.toString());
        } catch (XPathExpressionException ex) {
            System.err.println("Could not compile XPath expression " + ex.getLocalizedMessage());
        }
        return figure;
    }
}
