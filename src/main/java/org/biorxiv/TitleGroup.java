package org.biorxiv;

import org.w3c.dom.Node;

import javax.annotation.Nullable;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import java.io.Serializable;

public class TitleGroup implements Serializable {
    protected String articleTitle;

    public void setArticleTitle(String title) {
        this.articleTitle = title;
    }

    public String getArticleTitle() {
        if (articleTitle == null) {
            articleTitle = "";
        }
        return this.articleTitle;
    }

    public String toString() {
        return this.articleTitle;
    }

    @Override
    public boolean equals(@Nullable Object other) {
        return other instanceof TitleGroup &&
                this.getArticleTitle().equals(((TitleGroup) other).getArticleTitle());
    }

    public static TitleGroup parse(Node node, XPath xPath) {
        TitleGroup group = new TitleGroup();
        try {
            Node titleNode = (Node) xPath.compile("article-title").evaluate(node, XPathConstants.NODE);
            if (titleNode != null) {
                group.setArticleTitle(titleNode.getTextContent());
            }
        } catch (XPathExpressionException ex) {
            System.err.println("Could not compile XPath expression " + ex.getLocalizedMessage());
        }
        return group;
    }
}
