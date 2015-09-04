package com.rei.trailregister.client;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;

public class BatchingTrailRegisterClient implements TrailRegisterClient {
	private static Logger logger = LoggerFactory.getLogger(BatchingTrailRegisterClient.class);
	
	public static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
	
	private Gson json = new Gson();
    private OkHttpClient client = new OkHttpClient();
	private String url;
	
	private volatile ConcurrentMap<String, ConcurrentMap<String, Integer>> data = new ConcurrentHashMap<>();

	private ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
	
	public BatchingTrailRegisterClient(String baseUrl, String app, String env, long interval, TimeUnit unit) {
		this.url = baseUrl + "/" + app + "/" + env;
		executor.scheduleWithFixedDelay(this::sendBatch, interval, interval, unit);
	}
	
	@Override
	public void recordUsage(String category, String key) {
		data.compute(category, (k, v) -> v == null ? new ConcurrentHashMap<>() : v)
				.compute(key, (k, v) -> v == null ? 1 : v + 1);
	}

	private void sendBatch() {
		if (data.isEmpty()) {
			return;
		}
		
		try {
			int response = post(data);
			if (response == 201) {
				data = new ConcurrentHashMap<>();
			}
		} catch (IOException e) {
			// don't replace if we failed, try again
			logger.warn("failed to send usage data", e);
		}
	}
	
	int post(Object body) throws IOException {
        Request request = new Request.Builder()
                .url(url)
                .post(RequestBody.create(JSON, json.toJson(body)))
                .build();

        return client.newCall(request).execute().code();
    }
}