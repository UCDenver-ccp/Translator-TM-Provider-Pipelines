package org.biorxiv;

import org.w3c.dom.Node;

import javax.annotation.Nullable;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import java.io.Serializable;

public class Front implements Serializable {

    protected ArticleMeta articleMeta;

    public ArticleMeta getArticleMeta() {
        return articleMeta;
    }

    public void setArticleMeta(ArticleMeta articleMeta) {
        this.articleMeta = articleMeta;
    }

    public String toString() {
        if (articleMeta == null) {
            return "";
        }
        return articleMeta.toString();
    }

    @Override
    public boolean equals(@Nullable Object other) {
        return other instanceof Front &&
                this.getArticleMeta().equals(((Front) other).getArticleMeta());
    }

    public static Front parse(Node frontNode, XPath xPath) {
        Front theFront = new Front();
        try {
            Node articleMetaNode = (Node) xPath.compile("article-meta").evaluate(frontNode, XPathConstants.NODE);
            theFront.setArticleMeta(ArticleMeta.parse(articleMetaNode, xPath));
        } catch (XPathExpressionException ex) {
            System.err.println("Could not compile XPath expression " + ex.getLocalizedMessage());
        }
        return theFront;
    }
}
