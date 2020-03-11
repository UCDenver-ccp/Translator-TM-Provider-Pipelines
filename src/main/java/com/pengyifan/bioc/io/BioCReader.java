package com.pengyifan.bioc.io;

import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;

import javax.xml.namespace.QName;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLResolver;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartDocument;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.apache.commons.lang3.Validate;
import org.codehaus.stax2.XMLEventReader2;
import org.codehaus.stax2.XMLInputFactory2;

import com.pengyifan.bioc.BioCAnnotation;
import com.pengyifan.bioc.BioCCollection;
import com.pengyifan.bioc.BioCDocument;
import com.pengyifan.bioc.BioCLocation;
import com.pengyifan.bioc.BioCNode;
import com.pengyifan.bioc.BioCPassage;
import com.pengyifan.bioc.BioCRelation;
import com.pengyifan.bioc.BioCSentence;

class BioCReader implements Closeable {

  enum Level {
    COLLECTION_LEVEL, DOCUMENT_LEVEL, PASSAGE_LEVEL, SENTENCE_LEVEL
  }

  BioCCollection collection;
  BioCDocument document;
  BioCPassage passage;
  BioCSentence sentence;
  XMLEventReader2 reader;
  private int state;

  Level level;

  protected BioCReader(Reader reader, Level level, XMLResolver resolver)
      throws FactoryConfigurationError, XMLStreamException {
    XMLInputFactory2 factory = (XMLInputFactory2) XMLInputFactory2
        .newInstance();
    factory.setProperty(XMLInputFactory.IS_REPLACING_ENTITY_REFERENCES, false);
    factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
    factory.setProperty(XMLInputFactory.IS_COALESCING, false);
    factory.setProperty(XMLInputFactory.IS_VALIDATING, false);
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, true);
    factory.setProperty(XMLInputFactory.RESOLVER, resolver);
    factory.setXMLResolver(resolver);
    this.reader = (XMLEventReader2) factory.createXMLEventReader(reader);
    this.level = level;
    state = 0;
  }

  @Override
  public void close()
      throws IOException {
    try {
      reader.close();
    } catch (XMLStreamException e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  private String getAttribute(StartElement startElement, String key) {
    return startElement.getAttributeByName(new QName(key)).getValue();
  }

  private String getText()
      throws XMLStreamException {
    XMLEvent event = reader.nextEvent();
    if (event.isCharacters()) {
      return event.asCharacters().getData();
    } else {
      return "";
    }
  }

  protected Object read()
      throws XMLStreamException {

    String localName = null;

    while (reader.hasNext()) {
      XMLEvent event = reader.nextEvent();
      switch (state) {
      case 0:
        if (event.isStartElement()) {
          StartElement startElement = event.asStartElement();
          localName = startElement.getName().getLocalPart();
          if (localName.equals("collection")) {
            state = 1;
          }
        } else if (event.isStartDocument()) {
          StartDocument startDocument = (StartDocument) event;
          collection = new BioCCollection();
          collection.setEncoding(startDocument.getCharacterEncodingScheme());
          collection.setVersion(startDocument.getVersion());
          collection.setStandalone(startDocument.isStandalone());
        } else if (event.getEventType() == XMLStreamConstants.DTD) {
//          DTD2 dtd = (DTD2) event;
        }
        break;
      case 1:
        if (event.isStartElement()) {
          StartElement startElement = event.asStartElement();
          localName = startElement.getName().getLocalPart();
          if (localName.equals("source")) {
            collection.setSource(getText());
          } else if (localName.equals("date")) {
            collection.setDate(getText());
          } else if (localName.equals("key")) {
            collection.setKey(getText());
          } else if (localName.equals("infon")) {
            collection.putInfon(
                getAttribute(startElement, "key"),
                getText());
          } else if (localName.equals("document")) {
            // read document
            document = new BioCDocument();
            state = 2;
          } else {
            // blank
          }
        }
        else if (event.isEndElement()) {
          EndElement endElement = event.asEndElement();
          localName = endElement.getName().getLocalPart();
          if (localName.equals("collection")) {
            sentence = null;
            passage = null;
            document = null;

            state = 0;
          }
          break;
        }
        break;
      case 2:
        if (event.isStartElement()) {
          StartElement startElement = event.asStartElement();
          localName = startElement.getName().getLocalPart();
          if (localName.equals("id")) {
            document.setID(getText());
          } else if (localName.equals("infon")) {
            document.putInfon(
                getAttribute(startElement, "key"),
                getText());
          } else if (localName.equals("passage")) {
            // read passage
            passage = new BioCPassage();
            state = 3;
          } else if (localName.equals("annotation")) {
            // read annotation
            document.addAnnotation(readAnnotation(startElement));
          } else if (localName.equals("relation")) {
            // read relation
            document.addRelation(readRelation(startElement));
          } else {
            // blank
          }
        }
        else if (event.isEndElement()) {
          EndElement endElement = event.asEndElement();
          localName = endElement.getName().getLocalPart();
          if (localName.equals("document")) {
            state = 1;
            if (level == Level.DOCUMENT_LEVEL) {
              return document;
            } else if (document != null) {
              collection.addDocument(document);
            }
          }
          break;
        }
        break;
      case 3:
        if (event.isStartElement()) {
          StartElement startElement = event.asStartElement();
          localName = startElement.getName().getLocalPart();
          if (localName.equals("offset")) {
            passage.setOffset(Integer.parseInt(getText()));
          } else if (localName.equals("text")) {
            passage.setText(getText());
          } else if (localName.equals("infon")) {
            passage.putInfon(
                getAttribute(startElement, "key"),
                getText());
          } else if (localName.equals("annotation")) {
            passage.addAnnotation(readAnnotation(startElement));
          } else if (localName.equals("relation")) {
            passage.addRelation(readRelation(startElement));
          } else if (localName.equals("sentence")) {
            // read sentence
            sentence = new BioCSentence();
            state = 4;
          } else {
            // blank
          }
        }
        else if (event.isEndElement()) {
          EndElement endElement = event.asEndElement();
          localName = endElement.getName().getLocalPart();
          if (localName.equals("passage")) {
            state = 2;
            if (level == Level.PASSAGE_LEVEL) {
              return passage;
            } else if (passage != null) {
              document.addPassage(passage);
            }
          }
          break;
        }
        break;
      case 4:
        if (event.isStartElement()) {
          StartElement startElement = event.asStartElement();
          localName = startElement.getName().getLocalPart();
          if (localName.equals("offset")) {
            sentence.setOffset(Integer.parseInt(getText()));
          } else if (localName.equals("text")) {
            sentence.setText(getText());
          } else if (localName.equals("infon")) {
            sentence.putInfon(
                getAttribute(startElement, "key"),
                getText());
          } else if (localName.equals("annotation")) {
            sentence.addAnnotation(readAnnotation(startElement));
          } else if (localName.equals("relation")) {
            sentence.addRelation(readRelation(startElement));
          } else {
            // blank
          }
        }
        else if (event.isEndElement()) {
          EndElement endElement = event.asEndElement();
          localName = endElement.getName().getLocalPart();
          if (localName.equals("sentence")) {
            state = 3;
            if (level == Level.SENTENCE_LEVEL) {
              return sentence;
            } else if (sentence != null) {
              passage.addSentence(sentence);
            }
          }
          break;
        }
      }
    }
    return collection;
  }

  private BioCAnnotation readAnnotation(StartElement annotationEvent)
      throws XMLStreamException {
    BioCAnnotation ann = new BioCAnnotation();
    ann.setID(getAttribute(annotationEvent, "id"));

    String localName = null;

    while (reader.hasNext()) {
      XMLEvent event = reader.nextEvent();
      if (event.isStartElement()) {
        StartElement startElement = event.asStartElement();
        localName = startElement.getName().getLocalPart();
        if (localName.equals("text")) {
          ann.setText(getText());
        } else if (localName.equals("infon")) {
          ann.putInfon(
              startElement.getAttributeByName(new QName("key")).getValue(),
              getText());
        } else if (localName.equals("location")) {
          ann.addLocation(new BioCLocation(
              Integer.parseInt(getAttribute(startElement, "offset")),
              Integer.parseInt(getAttribute(startElement, "length"))));
        }
      }
      else if (event.isEndElement()) {
        EndElement endElement = event.asEndElement();
        localName = endElement.getName().getLocalPart();
        if (localName.equals("annotation")) {
          return ann;
        }
      }
    }
    Validate.isTrue(false, "should not reach here");
    return null;
  }

  private BioCRelation readRelation(StartElement relationEvent)
      throws XMLStreamException {
    BioCRelation rel = new BioCRelation();
    rel.setID(getAttribute(relationEvent, "id"));

    String localName = null;

    while (reader.hasNext()) {
      XMLEvent event = reader.nextEvent();
      if (event.isStartElement()) {
        StartElement startElement = event.asStartElement();
        localName = startElement.getName().getLocalPart();
        if (localName.equals("infon")) {
          rel.putInfon(
              getAttribute(startElement, "key"),
              getText());
        } else if (localName.equals("node")) {
          BioCNode node = new BioCNode(getAttribute(startElement, "refid"),
              getAttribute(startElement, "role"));
          rel.addNode(node);
        }
      }
      else if (event.isEndElement()) {
        EndElement endElement = event.asEndElement();
        localName = endElement.getName().getLocalPart();
        if (localName.equals("relation")) {
          return rel;
        }
      }
    }
    Validate.isTrue(false, "should not reach here");
    return null;
  }
}
