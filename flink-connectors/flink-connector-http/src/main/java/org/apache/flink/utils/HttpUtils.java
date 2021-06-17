package org.apache.flink.utils;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import org.apache.flink.annotation.Internal;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Internal
public class HttpUtils {

	private static final ObjectMapper objectMapper = new ObjectMapper();

	public static boolean isPostRequest(String requestMethod) {
		return StringUtils.equalsIgnoreCase("POST", requestMethod);
	}

	public static HttpPost wrapPostRequest(String requestUrl,
										   Map<String, String> requestHeaders,
										   List<String> requestParams,
										   List<Object> requestParamValues) throws IOException {
		HttpPost post = new HttpPost(requestUrl);
		if (MapUtils.isNotEmpty(requestHeaders)) {
			for (String key : requestHeaders.keySet()) {
				post.addHeader(key, requestHeaders.get(key));
			}
		}
		Map<String, Object> request = new HashMap<>();
		for (int i = 0; i < requestParamValues.size(); i++) {
			request.put(requestParams.get(i), requestParamValues.get(i));
		}
		System.out.println("request: " + request);
		StringEntity entity = new StringEntity(
			objectMapper.writeValueAsString(request),
			StandardCharsets.UTF_8);
		post.setEntity(entity);
		return post;
	}

	public static HttpGet wrapGetRequest(String requestUrl,
										   Map<String, String> requestHeaders,
										   List<String> requestParams,
										   List<Object> requestParamValues) throws Exception {
		URIBuilder uriBuilder = new URIBuilder(requestUrl);
		for (int i = 0; i < requestParamValues.size(); i++) {
			uriBuilder.addParameter(requestParams.get(i), String.valueOf(requestParamValues.get(i)));
		}
		HttpGet get = new HttpGet(uriBuilder.build());
		if (MapUtils.isNotEmpty(requestHeaders)) {
			for (String key : requestHeaders.keySet()) {
				get.addHeader(key, requestHeaders.get(key));
			}
		}
		return get;
	}
}
