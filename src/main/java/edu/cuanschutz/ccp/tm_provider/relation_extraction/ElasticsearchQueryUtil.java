package edu.cuanschutz.ccp.tm_provider.relation_extraction;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import edu.cuanschutz.ccp.tm_provider.relation_extraction.ElasticsearchToBratExporter.Sentence;
import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.common.io.ClassPathUtil;
import edu.ucdenver.ccp.file.conversion.TextDocument;

public class ElasticsearchQueryUtil {

	public static void sentenceSearch(String sentence, String indexName, String elasticUrl, int elasticPort,
			String elasticApiKey) throws ElasticsearchException, IOException {

		Header[] defaultHeaders = new Header[] { new BasicHeader("Authorization", "ApiKey " + elasticApiKey) };

		try (RestClient restClient = RestClient.builder(new HttpHost(elasticUrl, elasticPort, "https"))
				.setDefaultHeaders(defaultHeaders).build()) {

			// Create the transport with a Jackson mapper
			RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());

			// And create the API client
			ElasticsearchClient client = new ElasticsearchClient(transport);

			BooleanResponse ping = client.ping();
//			System.out.println("HEALTH: " + client.cluster().health().clusterName());
			System.out.println("PING: " + ping.value());

			String queryJson = buildSentenceQuery(sentence);
			System.out.println("QUERY:\n" + queryJson);

			Query query = new Query.Builder().withJson(new ByteArrayInputStream(queryJson.getBytes())).build();

			Time.Builder tb = new Time.Builder();
			Time t = tb.time("1m").build();

			SearchRequest.Builder searchBuilder = new SearchRequest.Builder().scroll(t).size(100).index(indexName)
					.query(query);
			SearchRequest request = searchBuilder.build();

			SearchResponse<Sentence> response = client.search(request, Sentence.class);

			String scrollId = response.scrollId();
			List<Hit<Sentence>> hits = response.hits().hits();

			for (Hit<Sentence> hit : hits) {
				Set<String> ontologyPrefixesToIncludeInSearchHits = new HashSet<String>();
				TextDocument td = ElasticsearchToBratExporter.deserializeAnnotatedText(hit.source().getAnnotatedText(),
						ontologyPrefixesToIncludeInSearchHits, null);

				System.out.println("HIT: " + td.getText());
			}

		}
	}

	private static String buildSentenceQuery(String sentenceText) throws IOException {

		// load annotatedText match template
		String matchTemplate = ClassPathUtil.getContentsFromClasspathResource(ElasticsearchToBratExporter.class,
				ElasticsearchToBratExporter.ELASTIC_ANNOTATEDTEXT_MATCH_TEMPLATE, CharacterEncoding.UTF_8);

		matchTemplate = matchTemplate.replace(
				ElasticsearchToBratExporter.ELASTIC_ANNOTATEDTEXT_MATCH_TEMPLATE_QUERY_PLACEHOLDER, sentenceText);
		matchTemplate = matchTemplate.replace(
				ElasticsearchToBratExporter.ELASTIC_ANNOTATEDTEXT_MATCH_TEMPLATE_BOOLEAN_OPERATOR_PLACEHOLDER, "and");

		return matchTemplate;

	}

	public static void main(String[] args) {
		String indexName = args[0];
		String elasticUrl = args[1];
		int elasticPort = Integer.parseInt(args[2]);
		String elasticApiKey = args[3];
		String sentence = "9-Phenanthrol , a TRPM4 inhibitor , protects isolated rat hearts from ischemia-reperfusion injury .";

		try {
			sentenceSearch(sentence, indexName, elasticUrl, elasticPort, elasticApiKey);
		} catch (ElasticsearchException | IOException e) {
			e.printStackTrace();
		}
	}

}
