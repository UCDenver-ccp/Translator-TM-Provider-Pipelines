package edu.cuanschutz.ccp.tm_provider.etl.fn;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.google.common.annotations.VisibleForTesting;

public class PCollectionUtil {

	public enum Delimiter {
		TAB("\t", "\\t"), PIPE("|", "\\|");

		private final String delimiter;
		private final String regex;

		private Delimiter(String delimiter, String regex) {
			this.delimiter = delimiter;
			this.regex = regex;
		}

		public String delimiter() {
			return this.delimiter;
		}

		public String regex() {
			return this.regex;
		}
	}

	public static PCollection<KV<String, String>> fromTwoColumnFiles(String description, Pipeline p, String filePattern,
			Delimiter delimiter, Compression compression) {
		PCollection<String> lines = p.apply(description, TextIO.read().from(filePattern).withCompression(compression));
		// parse each line to get a key/value pair
		PCollection<KV<String, String>> column0Tocolumn1 = lines.apply(ParDo.of(new DoFn<String, KV<String, String>>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				String element = c.element();
				String[] cols = element.split(delimiter.regex());
				if (cols.length != 2) {
					throw new IllegalArgumentException("Unable to split line into two columns. Delimiter=" + delimiter.name() + " Line=" + element);
				}
				c.output(KV.of(cols[0], cols[1]));
			}
		}));
		return column0Tocolumn1;
	}

	/**
	 * @param p
	 * @param filePattern
	 * @param delimiter
	 * @return
	 */
	public static PCollection<KV<String, Set<String>>> fromKeyToSetTwoColumnFiles(String description, Pipeline p, String filePattern,
			Delimiter fileDelimiter, Delimiter setDelimiter, Compression compression) {
		PCollection<String> lines = p.apply(description ,TextIO.read().from(filePattern).withCompression(compression));
		// parse each line to get a key/value pair
		PCollection<KV<String, Set<String>>> column0Tocolumn1 = lines
				.apply(ParDo.of(new DoFn<String, KV<String, Set<String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						String element = c.element();

						KV<String, Set<String>> kv = getKV(fileDelimiter, setDelimiter, element);

						c.output(kv);
					}

				}));

		// in case a key appears in more than one file, group by keys and combine
		// results
		PCollection<KV<String, Iterable<Set<String>>>> groupedById = column0Tocolumn1.apply("group by id",
				GroupByKey.<String, Set<String>>create());

		PCollection<KV<String, Set<String>>> groupedOutput = groupedById
				.apply(ParDo.of(new DoFn<KV<String, Iterable<Set<String>>>, KV<String, Set<String>>>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						KV<String, Iterable<Set<String>>> element = c.element();

						Set<String> aggregateSet = new HashSet<String>();
						for (Set<String> set : element.getValue()) {
							aggregateSet.addAll(set);
						}

						c.output(KV.of(element.getKey(), aggregateSet));
					}
				}));
		return groupedOutput;
	}

	@VisibleForTesting
	protected static KV<String, Set<String>> getKV(Delimiter fileDelimiter, Delimiter setDelimiter, String element) {
		int indexOfDelimiter = element.indexOf(fileDelimiter.delimiter());
		String col0 = element.substring(0, indexOfDelimiter);

		Set<String> set = new HashSet<String>(
				Arrays.asList(element.substring(indexOfDelimiter + 1).split(setDelimiter.regex())));

		KV<String, Set<String>> kv = KV.of(col0, set);
		return kv;
	}

//	public static void main(String[] args) throws IOException {
//		for (Iterator<File> fileIterator = FileUtil.getFileIterator(
//				new File("/Users/bill/projects/ncats-translator/craft-resources/mapping-files"), false); fileIterator
//						.hasNext();) {
//			File f = fileIterator.next();
//
//			System.out.println(f.getName());
//			for (StreamLineIterator lineIter = new StreamLineIterator(new GZIPInputStream(new FileInputStream(f)),
//					CharacterEncoding.UTF_8, null); lineIter.hasNext();) {
//				String line = lineIter.next().getText();
//
//				int indexOfDelimiter = line.indexOf(Delimiter.TAB.delimiter);
//				String col0 = line.substring(0, indexOfDelimiter);
//
//				Set<String> set = new HashSet<String>(
//						Arrays.asList(line.substring(indexOfDelimiter + 1).split(Delimiter.TAB.regex())));
//			}
//		}
//	}

}
