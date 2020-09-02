package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

public class ToEntityFnUtils {

	public static Set<String> getCollections(String collection, Function<String, String> collectionFn, String docId) {
		Set<String> collections = new HashSet<String>();
		if (collection != null && !collection.isEmpty()) {
			collections.add(collection);
		}
		if (collectionFn != null) {
			String anotherCollection = collectionFn.apply(docId);
			collections.add(anotherCollection);
		}
		return collections;
	}

}
