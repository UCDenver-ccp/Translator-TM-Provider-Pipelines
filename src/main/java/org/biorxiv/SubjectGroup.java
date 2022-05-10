package org.biorxiv;

import org.w3c.dom.Node;

import javax.annotation.Nullable;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import java.io.Serializable;

public class SubjectGroup implements Serializable {
    protected String subjectGroupType;
    protected String subject;

    public String getSubjectGroupType() {
        if (subjectGroupType == null) {
            subjectGroupType = "";
        }
        return subjectGroupType;
    }

    public void setSubjectGroupType(String subjectGroupType) {
        this.subjectGroupType = subjectGroupType;
    }

    public String getSubject() {
        if (subject == null) {
            subject = "";
        }
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String toString() {
        return String.format("%s\t%s", subjectGroupType, subject);
    }

    @Override
    public boolean equals(@Nullable Object other) {
        return other instanceof SubjectGroup &&
                this.getSubject().equals(((SubjectGroup) other).getSubject()) &&
                this.getSubjectGroupType().equals(((SubjectGroup) other).getSubjectGroupType());
    }

    public static SubjectGroup parse(Node node, XPath xPath) {
        SubjectGroup group = new SubjectGroup();
        group.setSubjectGroupType(node.getAttributes().getNamedItem("subj-group-type").getNodeValue());
        try {
            Node subjectNode = (Node) xPath.compile("subject").evaluate(node, XPathConstants.NODE);
            if (subjectNode != null) {
                group.setSubject(subjectNode.getTextContent());
            }
        } catch (XPathExpressionException ex) {
            System.err.println("Could not compile XPath expression " + ex.getLocalizedMessage());
        }
        return group;
    }
}