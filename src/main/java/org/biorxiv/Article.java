package org.biorxiv;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nullable;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import java.io.Serializable;

public class Article implements Serializable {

    protected Front front;
    protected Body body;

    public Front getFront() {
        return front;
    }

    public void setFront(Front front) {
        this.front = front;
    }

    public Body getBody() {
        return body;
    }

    public void setBody(Body body) {
        this.body = body;
    }

    public static NodeList getArticles(Document document, XPath xPath) {
        try {
            return (NodeList) xPath.compile("/articles/article")
                    .evaluate(document, XPathConstants.NODESET);
        } catch (XPathExpressionException ex) {
            System.err.println("Could not compile articles XPath expression " + ex.getLocalizedMessage());
        }
        return null;
    }

    public static Article parse(Node document, XPath xPath) {
        Article theArticle = new Article();
        try {
            Node frontNode = (Node) xPath.compile("front").evaluate(document, XPathConstants.NODE);
            Node bodyNode = (Node) xPath.compile("body").evaluate(document, XPathConstants.NODE);
            theArticle.setFront(Front.parse(frontNode, xPath));
            theArticle.setBody(Body.parse(bodyNode, xPath));
        } catch (XPathExpressionException ex) {
            System.err.println("Could not compile front or body XPath expression " + ex.getLocalizedMessage());
        }
        return theArticle;
    }

    @Override
    public boolean equals(@Nullable Object other) {
        return other instanceof Article &&
                this.getFront().equals(((Article) other).getFront()) &&
                this.getBody().equals(((Article) other).getBody());
    }

    public String toString() {
        return front.toString() + "\n\n" + body.toString();
    }
}