package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

public class ToEntityFnUtils {

	public static Set<String> getCollections(String collection, Function<String, String> collectionFn, String docId) {
		Set<String> collections = new HashSet<String>();

		/* add a collection that is the date */
		collections.add(getDateCollectionName());

		if (collection != null && !collection.isEmpty()) {
			collections.add(collection);
		}
		if (collectionFn != null) {
			String anotherCollection = collectionFn.apply(docId);
			if (anotherCollection != null) {
				collections.add(anotherCollection);
			}
		}
		return collections;
	}

	protected static String getDateCollectionName() {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd");
		Date date = new Date(System.currentTimeMillis());
		return dateFormat.format(date);
	}

}
