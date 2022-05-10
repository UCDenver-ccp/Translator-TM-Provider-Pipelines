package org.biorxiv;

import org.w3c.dom.Node;

import javax.annotation.Nullable;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import java.io.Serializable;

public class PubDate implements Serializable {

    protected String pubType;
    protected String year;

    public String getPubType() {
        return pubType;
    }

    public void setPubType(String pubType) {
        this.pubType = pubType;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String toString() {
        return String.format("%s\t%s", pubType, year);
    }

    @Override
    public boolean equals(@Nullable Object other) {
        return other instanceof PubDate &&
                this.getPubType().equals(((PubDate) other).pubType) &&
                this.getYear().equals(((PubDate) other).getYear());
    }

    public static PubDate parse(Node node, XPath xPath) {
        PubDate pubDate = new PubDate();
        pubDate.setPubType(node.getAttributes().getNamedItem("pub-type").getNodeValue());
        try {
            Node yearNode = (Node) xPath.compile("year").evaluate(node, XPathConstants.NODE);
            if (yearNode != null) {
                pubDate.setYear(yearNode.getTextContent());
            }
        } catch (XPathExpressionException ex) {
            System.err.println("Could not compile XPath expression " + ex.getLocalizedMessage());
        }
        return pubDate;
    }
}
