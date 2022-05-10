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

public class ArticleCategories implements Serializable {
    protected List<SubjectGroup> subjectGroups;

    public List<SubjectGroup> getSubjectGroups() {
        if (subjectGroups == null) {
            return Collections.emptyList();
        }
        return subjectGroups;
    }

    public void setSubjectGroups(List<SubjectGroup> subjectGroups) {
        this.subjectGroups = subjectGroups;
    }

    public String toString() {
        StringBuilder fullTextBuilder = new StringBuilder();
        for (SubjectGroup group : this.getSubjectGroups()) {
            fullTextBuilder.append(group.toString());
            fullTextBuilder.append('\n');
        }
        return fullTextBuilder.toString();
    }

    @Override
    public boolean equals(@Nullable Object other) {
        return other instanceof ArticleCategories &&
                this.getSubjectGroups().equals(((ArticleCategories) other).getSubjectGroups());
    }

    public static ArticleCategories parse(Node node, XPath xPath) {
        ArticleCategories articleCategories = new ArticleCategories();
        try {
            NodeList subjectGroupNodeList = (NodeList) xPath.compile("subj-group").evaluate(node, XPathConstants.NODESET);
            if (subjectGroupNodeList.getLength() > 0) {
                List<SubjectGroup> groups = new ArrayList<>();
                for (int i = 0; i < subjectGroupNodeList.getLength(); i++) {
                    Node subjectGroupNode = subjectGroupNodeList.item(i);
                    groups.add(SubjectGroup.parse(subjectGroupNode, xPath));
                }
                articleCategories.setSubjectGroups(groups);
            }
        } catch (XPathExpressionException ex) {
            System.err.println("Could not compile XPath expression " + ex.getLocalizedMessage());
        }
        return articleCategories;
    }
}
