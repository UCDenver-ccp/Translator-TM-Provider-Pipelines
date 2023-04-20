package edu.cuanschutz.ccp.tm_provider.etl.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdTokenCredentials;
import com.google.auth.oauth2.IdTokenProvider;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import lombok.Data;

/**
 * Simple utility for submitting an HTTP POST request
 */
@Data
public class HttpPostUtil {

	private static final Logger logger = org.apache.log4j.Logger.getLogger(HttpPostUtil.class);

	private final String targetUri;

	/**
	 * submit makes a GET request to the specified Cloud Run or Cloud Functions
	 * endpoint, serviceUrl (must be a complete URL), by authenticating with an Id
	 * token retrieved from Application Default Credentials.
	 * 
	 * Based on code from:
	 * https://cloud.google.com/run/docs/authenticating/service-to-service#console-ui
	 * 
	 * @param payload
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public String submit(String payload) throws IOException {

		HttpResponse response = makeHttpRequest(payload);
//		/*
//		 * occasionally the server does not respond, or does not respond in time and an
//		 * exception is thrown. In these cases we will catch the exception, wait 5
//		 * seconds, and retry up to 3 times total before officially failing.
//		 */
//		int tryCount = 0;
//		HttpResponse response = null;
//		while (tryCount++ < 4) {
//			try {
//				response = makeHttpRequest(payload);
//				// request was successful so break out of the while loop
//				break;
//			} catch (HttpResponseException e) {
//				if (tryCount == 4) {
//					throw new HttpResponseException(response);
//				}
//				/* sleep for 5s before trying again */
//				try {
//					Thread.sleep(5000);
//				} catch (InterruptedException e1) {
//					throw new IOException(e1);
//				}
//			}
//		}
//		if (tryCount > 1) {
//			logger.info("TMPLOG -- recovered HTTPReponse after sleeping.");
//		}

		ByteArrayOutputStream os = new ByteArrayOutputStream();
		try {
			response.download(os);
			return os.toString(CharacterEncoding.UTF_8.getCharacterSetName());
		} finally {
			if (os != null) {
				os.close();
			}
		}
	}

	private HttpResponse makeHttpRequest(String payload) throws IOException {
		HttpResponse response;
		GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
		if (!(credentials instanceof IdTokenProvider)) {
			throw new IllegalArgumentException("Credentials are not an instance of IdTokenProvider.");
		}
		IdTokenCredentials tokenCredential = IdTokenCredentials.newBuilder()
				.setIdTokenProvider((IdTokenProvider) credentials).setTargetAudience(targetUri).build();

		GenericUrl genericUrl = new GenericUrl(targetUri);
		HttpCredentialsAdapter adapter = new HttpCredentialsAdapter(tokenCredential);
		HttpTransport transport = new NetHttpTransport();
		HttpContent content = new ByteArrayContent("application/text; charset=utf-8",
				payload.getBytes(CharacterEncoding.UTF_8.getCharacterSetName()));
		HttpRequest request = transport.createRequestFactory(adapter).buildPostRequest(genericUrl, content);

		response = request.execute();
		return response;
	}

//	public String submit(String payload) throws IOException {
//		int timeout = 5; // seconds
//		RequestConfig config = RequestConfig.custom().setConnectTimeout(timeout * 1000)
//				.setConnectionRequestTimeout(timeout * 1000).setSocketTimeout(timeout * 1000).build();
//		try (CloseableHttpClient client = HttpClientBuilder.create().setDefaultRequestConfig(config).build()) {
//
//			HttpPost post = new HttpPost(targetUri);
//
//			EntityBuilder builder = EntityBuilder.create();
//			builder.setContentType(ContentType.TEXT_PLAIN);
//			builder.setBinary(payload.getBytes());
//			HttpEntity entity = builder.build();
//
//			post.setEntity(entity);
//			HttpResponse response = client.execute(post);
//
//			response.getStatusLine().getStatusCode();
//			return EntityUtils.toString(response.getEntity());
//		}
//
//	}

}
