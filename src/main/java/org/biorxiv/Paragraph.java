package org.biorxiv;

import org.w3c.dom.Node;

import javax.annotation.Nullable;
import javax.xml.xpath.XPath;
import java.io.Serializable;

public class Paragraph implements Serializable {

    protected String value;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public boolean equals(@Nullable Object other) {
        return other instanceof Paragraph &&
                this.getValue().equals(((Paragraph) other).getValue());
    }

    @Override
    public String toString() {
        return this.value;
    }

    // This is trivial for now, but if we ever need to take out the XRef elements or do any other processing
    // it will happen here.
    public static Paragraph parse(Node paragraphNode, XPath xPath) {
        Paragraph paragraph = new Paragraph();
        paragraph.setValue(paragraphNode.getTextContent());
        return paragraph;
    }
}
