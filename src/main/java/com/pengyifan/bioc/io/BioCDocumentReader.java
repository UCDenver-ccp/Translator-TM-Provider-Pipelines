package com.pengyifan.bioc.io;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLResolver;
import javax.xml.stream.XMLStreamException;

import com.pengyifan.bioc.BioCCollection;
import com.pengyifan.bioc.BioCDocument;
import com.pengyifan.bioc.io.BioCReader.Level;

/**
 * Reads the BioC file sequentially into BioCDocument every time the method
 * {@link #readDocument} is called. For example,
 * <p>
 * 
 * <pre>
 * BioCDocumentReader reader = new BioCDocumentReader(&quot;foo.xml&quot;);
 * BioCCollection collection = reader.readCollectionInfo();
 * BioCDocument doc = null;
 * while ((doc = reader.readDocument()) != null) {
 * 	collection.addDocument(doc);
 * }
 * reader.close();
 * </pre>
 * 
 * @since 1.0.0
 * @see BioCCollectionReader
 * @author Yifan Peng
 */
public class BioCDocumentReader implements Closeable {

	private static XMLResolver resolver;
	private BioCReader reader;

	/**
	 * Creates a new BioCDocumentReader, given the File to read from.
	 * 
	 * @param file     the File to read from
	 * @param resolver
	 * @throws FactoryConfigurationError if a factory configuration error occurs
	 * @throws XMLStreamException        if an unexpected processing error occurs
	 * @throws FileNotFoundException     if the file does not exist, is a directory
	 *                                   rather than a regular file, or for some
	 *                                   other reason cannot be opened for reading.
	 */
	public BioCDocumentReader(File file, XMLResolver resolver)
			throws FactoryConfigurationError, XMLStreamException, FileNotFoundException {
		this(new FileReader(file), resolver);
	}

	/**
	 * Creates a new BioCDocumentReader, given the Path to read from.
	 *
	 * @param path     the file path to read from
	 * @param resolver
	 * @throws FactoryConfigurationError if a factory configuration error occurs
	 * @throws XMLStreamException        if an unexpected processing error occurs
	 * @throws IOException               if an I/O exception of some sort has
	 *                                   occurred
	 */
	public BioCDocumentReader(Path path, XMLResolver resolver)
			throws FactoryConfigurationError, XMLStreamException, IOException {
		this(Files.newBufferedReader(path), resolver);
	}

	/**
	 * Creates an BioCDocumentReader that uses the input stream in.
	 * 
	 * @param in       an InputStream
	 * @param resolver
	 * @throws FactoryConfigurationError if a factory configuration error occurs
	 * @throws XMLStreamException        if an unexpected processing error occurs
	 */
	public BioCDocumentReader(InputStream in, XMLResolver resolver)
			throws FactoryConfigurationError, XMLStreamException {
		this(new InputStreamReader(in), resolver);
	}

	/**
	 * Creates an BioCDocumentReader that uses the reader in.
	 * 
	 * @param in       a Reader
	 * @param resolver
	 * @throws FactoryConfigurationError if a factory configuration error occurs
	 * @throws XMLStreamException        if an unexpected processing error occurs
	 */
	public BioCDocumentReader(Reader in, XMLResolver resolver) throws FactoryConfigurationError, XMLStreamException {
		reader = new BioCReader(in, Level.DOCUMENT_LEVEL, resolver);
		reader.read();
	}

	/**
	 * Creates a new BioCDocumentReader, given the name of the file to read from.
	 * 
	 * @param fileName the name of the file to read from
	 * @throws FactoryConfigurationError if a factory configuration error occurs
	 * @throws XMLStreamException        if an unexpected processing error occurs
	 * @throws FileNotFoundException     if the file does not exist, is a directory
	 *                                   rather than a regular file, or for some
	 *                                   other reason cannot be opened for reading.
	 */
	public BioCDocumentReader(String fileName)
			throws FactoryConfigurationError, XMLStreamException, FileNotFoundException {
		this(new FileReader(fileName), resolver);
	}

	/**
	 * Closes the reader and releases any system resources associated with it. Once
	 * the reader has been closed, further readDocument() invocations will throw an
	 * IOException. Closing a previously closed reader has no effect.
	 */
	@Override
	public void close() throws IOException {
		reader.close();
	}

	/**
	 * Reads the collection information: encoding, version, DTD, source, date, key,
	 * infons, etc.
	 * 
	 * @return the BioC collection that contains only information
	 * @throws XMLStreamException if an unexpected processing error occurs
	 */
	public BioCCollection readCollectionInfo() throws XMLStreamException {
		return reader.collection;
	}

	/**
	 * Reads one BioC document from the XML file.
	 * 
	 * @return the BioC document
	 * @throws XMLStreamException if an unexpected processing error occurs
	 */
	public BioCDocument readDocument() throws XMLStreamException {

		if (reader.document != null) {
			BioCDocument thisDocument = reader.document;
			reader.read();
			return thisDocument;
		} else {
			return null;
		}
	}
}
