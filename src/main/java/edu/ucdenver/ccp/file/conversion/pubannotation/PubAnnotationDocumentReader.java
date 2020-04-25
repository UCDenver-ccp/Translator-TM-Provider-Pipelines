package edu.ucdenver.ccp.file.conversion.pubannotation;

/*-
 * #%L
 * Colorado Computational Pharmacology's file conversion
 * 						project
 * %%
 * Copyright (C) 2019 Regents of the University of Colorado
 * %%
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 * 
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * 3. Neither the name of the Regents of the University of Colorado nor the names of its contributors
 *    may be used to endorse or promote products derived from this software without
 *    specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.io.StreamUtil;
import edu.ucdenver.ccp.file.conversion.DocumentReader;
import edu.ucdenver.ccp.file.conversion.TextDocument;
import edu.ucdenver.ccp.file.conversion.pubannotation.PubAnnotationDocumentWriter.Denotation;
import edu.ucdenver.ccp.file.conversion.pubannotation.PubAnnotationDocumentWriter.Document;
import edu.ucdenver.ccp.file.conversion.pubannotation.PubAnnotationDocumentWriter.Relation;
import edu.ucdenver.ccp.file.conversion.util.DocumentReaderUtil;
import edu.ucdenver.ccp.nlp.core.annotation.Span;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotation;
import edu.ucdenver.ccp.nlp.core.annotation.TextAnnotationFactory;
import edu.ucdenver.ccp.nlp.core.mention.impl.DefaultClassMention;
import lombok.Data;

public class PubAnnotationDocumentReader extends DocumentReader {

	@Override
	public TextDocument readDocument(String sourceId, String sourceDb, InputStream inputStream,
			InputStream documentTextStream, CharacterEncoding encoding) throws IOException {
		String documentText = StreamUtil.toString(new InputStreamReader(documentTextStream, encoding.getDecoder()));
		TextDocument td = new TextDocument(sourceId, sourceDb, documentText);
		List<TextAnnotation> annotations = getAnnotations(inputStream, encoding);
		DocumentReaderUtil.validateSpans(annotations, documentText, sourceId);
		td.addAnnotations(annotations);
		return td;
	}

	public static List<TextAnnotation> getAnnotations(InputStream pubAnnotationStream, CharacterEncoding encoding) {
		TextAnnotationFactory factory = TextAnnotationFactory.createFactoryWithDefaults();

		Map<String, TextAnnotation> idToAnnotMap = new HashMap<String, TextAnnotation>();

		Gson gson = new Gson();
		Document document = gson.fromJson(new InputStreamReader(pubAnnotationStream, encoding.getDecoder()),
				Document.class);

		System.out.println("DOC: " + document.toString());

		String documentText = document.getText();

		/* create an annotation for every denotation */
		if (document.getDenotations() != null) {
			for (Denotation denot : document.getDenotations()) {
				TextAnnotation annot = factory.createAnnotation(denot.getSpan().getBegin(), denot.getSpan().getEnd(),
						documentText.substring(denot.getSpan().getBegin(), denot.getSpan().getEnd()),
						new DefaultClassMention(denot.getObj()));
				idToAnnotMap.put(denot.getId(), annot);
			}
		}

		/* now add relations */
		if (document.getRelations() != null) {
			for (Relation rel : document.getRelations()) {
				TextAnnotation subjAnnot = idToAnnotMap.get(rel.getSubj());
				TextAnnotation objAnnot = idToAnnotMap.get(rel.getObj());
				String relationType = rel.getPred();

				if (relationType.equals(PubAnnotationDocumentWriter.LEXICALLY_CHAINED_PREDICATE)) {
					/*
					 * then the subj and obj annotations need to be combined b/c they are a
					 * discontinuous span annotation
					 */

					Span span = objAnnot.getAggregateSpan();
					subjAnnot.addSpan(span);
					idToAnnotMap.remove(rel.getObj());
				} else {
					DocumentReader.createAnnotationRelation(subjAnnot, objAnnot, relationType);
				}
			}
		}

		return new ArrayList<TextAnnotation>(idToAnnotMap.values());
	}

}
