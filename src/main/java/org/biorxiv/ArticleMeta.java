package org.biorxiv;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ArticleMeta implements Serializable {

    protected List<ArticleID> articleIDs;
    protected ArticleCategories articleCategories;
    protected TitleGroup titleGroup;
    protected List<PubDate> pubDate;
    protected List<Abstract> abstracts;

    public List<ArticleID> getArticleIDs() {
        if (articleIDs == null) {
            return Collections.emptyList();
        }
        return articleIDs;
    }

    public void setArticleIDs(List<ArticleID> articleIDs) {
        this.articleIDs = articleIDs;
    }

    public ArticleCategories getArticleCategories() {
        if (articleCategories == null) {
            articleCategories = new ArticleCategories();
        }
        return articleCategories;
    }

    public void setArticleCategories(ArticleCategories articleCategories) {
        this.articleCategories = articleCategories;
    }

    public TitleGroup getTitleGroup() {
        if (titleGroup == null) {
            titleGroup = new TitleGroup();
        }
        return titleGroup;
    }

    public void setTitleGroup(TitleGroup titleGroup) {
        this.titleGroup = titleGroup;
    }

    public List<PubDate> getPubDate() {
        if (pubDate == null) {
            return Collections.emptyList();
        }
        return pubDate;
    }

    public void setPubDate(List<PubDate> pubDate) {
        this.pubDate = pubDate;
    }

    public List<Abstract> getAbstracts() {
        if (abstracts == null) {
            return Collections.emptyList();
        }
        return abstracts;
    }

    public void setAbstracts(List<Abstract> abstracts) {
        this.abstracts = abstracts;
    }

    public String toString() {
        StringBuilder fullTextBuilder = new StringBuilder();
        if (titleGroup != null) {
            fullTextBuilder.append(titleGroup);
            fullTextBuilder.append('\n');
        }
        for (ArticleID id : this.getArticleIDs()) {
            if (id.getPubIdType().equals("doi")) {
                fullTextBuilder.append(id.getValue());
                fullTextBuilder.append('\t');
            }
        }
        fullTextBuilder.append('\n');
        for (PubDate date : this.getPubDate()) {
            fullTextBuilder.append(date);
            fullTextBuilder.append('\n');
        }
        this.getAbstracts().forEach(fullTextBuilder::append);
        if (this.articleCategories != null) {
            fullTextBuilder.append(articleCategories);
        }
        return fullTextBuilder.toString();
    }

    public static ArticleMeta parse(Node articleMetaNode, XPath xPath) {
        ArticleMeta articleMeta = new ArticleMeta();
        try {
            NodeList articleIDNodeList = (NodeList) xPath.compile("article-id").evaluate(articleMetaNode, XPathConstants.NODESET);
            if (articleIDNodeList.getLength() > 0) {
                List<ArticleID> ids = new ArrayList<>();
                for (int i = 0; i < articleIDNodeList.getLength(); i++) {
                    Node articleIdNode = articleIDNodeList.item(i);
                    ids.add(ArticleID.parse(articleIdNode, xPath));
                }
                articleMeta.setArticleIDs(ids);
            }
        } catch (XPathExpressionException ex) {
            System.err.println("Could not compile XPath expression " + ex.getLocalizedMessage());
        }
        try {
            Node articleCategoriesNode = (Node) xPath.compile("article-categories").evaluate(articleMetaNode, XPathConstants.NODE);
            if (articleCategoriesNode != null) {
                articleMeta.setArticleCategories(ArticleCategories.parse(articleCategoriesNode, xPath));
            }
        } catch (XPathExpressionException ex) {
            System.err.println("Could not compile XPath expression " + ex.getLocalizedMessage());
        }
        try {
            Node titleGroupNode = (Node) xPath.compile("title-group").evaluate(articleMetaNode, XPathConstants.NODE);
            if (titleGroupNode != null) {
                articleMeta.setTitleGroup(TitleGroup.parse(titleGroupNode, xPath));
            }
        } catch (XPathExpressionException ex) {
            System.err.println("Could not compile XPath expression " + ex.getLocalizedMessage());
        }
        try {
            NodeList pubDateNodeList = (NodeList) xPath.compile("pub-date").evaluate(articleMetaNode, XPathConstants.NODESET);
            if (pubDateNodeList.getLength() > 0) {
                List<PubDate> dates = new ArrayList<>();
                for (int i = 0; i < pubDateNodeList.getLength(); i++) {
                    Node pubDateNode = pubDateNodeList.item(i);
                    dates.add(PubDate.parse(pubDateNode, xPath));
                }
                articleMeta.setPubDate(dates);
            }
        } catch (XPathExpressionException ex) {
            System.err.println("Could not compile XPath expression " + ex.getLocalizedMessage());
        }
        try {
            NodeList abstractNodeList = (NodeList) xPath.compile("abstract").evaluate(articleMetaNode, XPathConstants.NODESET);
            if (abstractNodeList.getLength() > 0) {
                List<Abstract> abstracts = new ArrayList<>();
                for (int i = 0; i < abstractNodeList.getLength(); i++) {
                    Node abstractNode = abstractNodeList.item(i);
                    abstracts.add(Abstract.parse(abstractNode, xPath));
                }
                articleMeta.setAbstracts(abstracts);
            }
        } catch (XPathExpressionException ex) {
            System.err.println("Could not compile XPath expression " + ex.getLocalizedMessage());
        }
        return articleMeta;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ArticleMeta &&
                this.getAbstracts().equals(((ArticleMeta) other).getAbstracts()) &&
                this.getArticleIDs().equals(((ArticleMeta) other).getArticleIDs()) &&
                this.getArticleCategories().equals(((ArticleMeta) other).getArticleCategories()) &&
                this.getPubDate().equals(((ArticleMeta) other).getPubDate()) &&
                this.getTitleGroup().equals(((ArticleMeta) other).getTitleGroup());
    }
}
