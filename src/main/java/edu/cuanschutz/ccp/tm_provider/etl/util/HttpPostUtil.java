package edu.cuanschutz.ccp.tm_provider.etl.util;

import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import lombok.Data;

/**
 * Simple utility for submitting an HTTP POST request
 */
@Data
public class HttpPostUtil {

	private final String targetUri;

	public String submit(String payload) throws IOException {
		HttpClient client = HttpClientBuilder.create().build();
		HttpPost post = new HttpPost(targetUri);

		EntityBuilder builder = EntityBuilder.create();
		builder.setContentType(ContentType.TEXT_PLAIN);
		builder.setBinary(payload.getBytes());
		HttpEntity entity = builder.build();

		post.setEntity(entity);
		HttpResponse response = client.execute(post);
		return EntityUtils.toString(response.getEntity());

	}

}
