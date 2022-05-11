package org.biorxiv;

import org.w3c.dom.Node;

import javax.annotation.Nullable;
import javax.xml.xpath.XPath;
import java.io.Serializable;

public class ArticleID implements Serializable {

    protected String pubIdType;
    protected String value;

    public String getPubIdType() {
        return pubIdType;
    }

    public void setPubIdType(String pubIdType) {
        this.pubIdType = pubIdType;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String toString() {
        return String.format("%s\t%s", pubIdType, value);
    }

    @Override
    public boolean equals(@Nullable Object other) {
        return other instanceof ArticleID &&
                this.getPubIdType().equals(((ArticleID) other).getPubIdType()) &&
                this.getValue().equals(((ArticleID) other).getValue());
    }

    public static ArticleID parse(Node node, XPath xPath) {
        ArticleID id = new ArticleID();
        id.setPubIdType(node.getAttributes().getNamedItem("pub-id-type").getNodeValue());
        id.setValue(node.getTextContent());
        return id;
    }
}
