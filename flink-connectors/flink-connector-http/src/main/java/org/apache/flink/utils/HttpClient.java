package org.apache.flink.utils;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * httpclient: common tool for send http client
 */
public class HttpClient {

  private static final Logger LOG = LoggerFactory.getLogger(HttpClient.class);
  private Integer requestTimeout;
  private Integer retryCount;
  private Integer socketTimeout;
  private Integer connectTimeout;

  public HttpClient() {
  }

  public HttpClient(Integer requestTimeout, Integer retryCount, Integer socketTimeout, Integer connectTimeout) {
    this.requestTimeout = requestTimeout;
    this.retryCount = retryCount;
    this.socketTimeout = socketTimeout;
    this.connectTimeout = connectTimeout;
  }

  /**
   * generate the httpclient
   *
   * @return
   */
  private CloseableHttpClient getHttpClient() {
    PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
    connManager.setMaxTotal(100);
    connManager.setDefaultMaxPerRoute(10);
    CloseableHttpClient httpClient = HttpClients.custom()
            .setConnectionManager(connManager)
            .setRetryHandler(
            	new DefaultHttpRequestRetryHandler(retryCount == null
					? HttpConstants.DEFAULT_RETRY_COUNT
					: retryCount, true))
            .build();
    return httpClient;
  }

  public static HttpClient getInstance(Integer requestTimeout) {
    return new HttpClient();
  }

  /**
   * the method to request
   *
   * @param requestBase: the request entity: HttpGet, HttpPost,etc
   * @return tuple2: _1 http status code, _2 the json result
   * @throws IOException
   */
  public Tuple2<Integer, String> request(HttpRequestBase requestBase) throws IOException {
    try (CloseableHttpClient httpClient = getHttpClient()) {
      RequestConfig requestConfig = RequestConfig.custom()
              .setSocketTimeout(socketTimeout == null ? HttpConstants.DEFAULT_SOCKET_TIMEOUT : socketTimeout)
              .setConnectTimeout(connectTimeout == null ? HttpConstants.DEFAULT_CONNECT_TIMEOUT : connectTimeout)
              .setConnectionRequestTimeout(requestTimeout == null ? HttpConstants.DEFAULT_REQUEST_TIMEOUT : requestTimeout)
              .build();
      requestBase.setConfig(requestConfig);
      HttpResponse response = httpClient.execute(requestBase);
      HttpEntity entity = response.getEntity();
      InputStream inputStream = entity.getContent();
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
      StringBuilder resultBuilder = new StringBuilder();
      String s = bufferedReader.readLine();
      while (s != null) {
        resultBuilder.append(s);
        s = bufferedReader.readLine();
      }
      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        LOG.error("Failed to do the request, with status code:{}", response.getStatusLine().getStatusCode());
      }
      return new Tuple2<>(response.getStatusLine().getStatusCode(), resultBuilder.toString());
    }
  }
}
