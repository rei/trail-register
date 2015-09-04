package com.rei.trailregister.client;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;

public class BatchingTrailRegisterClient implements TrailRegisterClient {
	private static Logger logger = LoggerFactory.getLogger(BatchingTrailRegisterClient.class);
	
	public static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
	
	private Gson json = new Gson();
    private OkHttpClient client = new OkHttpClient();
	private String baseUrl;
	
	// yes, holy crap
	private ConcurrentMap<String, ConcurrentMap<String, ConcurrentMap<String, Integer>>> data = new ConcurrentHashMap<>();
	

	private ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
	
	public BatchingTrailRegisterClient(String baseUrl, long interval, TimeUnit unit) {
		this.baseUrl = baseUrl;
		executor.scheduleWithFixedDelay(this::sendBatch, interval, interval, unit);
	}
	
	@Override
	public void recordUsage(String app, String env, String category, String key) {
	    String appEnvKey = app + "|" + env;
	    
		data.compute(appEnvKey, (k, v) -> v == null ? new ConcurrentHashMap<>() : v)
		    .compute(category, (k, v) -> v == null ? new ConcurrentHashMap<>() : v)
			.compute(key, (k, v) -> v == null ? 1 : v + 1);
	}
	
	@Override
    public int getUsages(String app, String env, String category, String key) {
        return get(path(app, env, category, key), new TypeToken<Integer>(){});
    }

    @Override
    public Map<String, Integer> getAllUsages(String app, String env, String category) {
        return get(path(app, env, category), new TypeToken<Map<String, Integer>>(){});
    }

    @Override
    public Map<String, Integer> getUsagesByDate(String app, String env, String category, String key) {
        return get(path(app, env, category, key + "?by_date=true"), new TypeToken<Map<String, Integer>>(){});
    }

	private void sendBatch() {
	    data.forEach((appEnv, appEnvData) -> {
	        if (appEnvData.isEmpty()) { return; }
	        
	        try {
		        String app = appEnv.substring(0, appEnv.indexOf("|"));
		        String env = appEnv.substring(appEnv.indexOf("|")+1, appEnv.length());
		        
		        int response = post(app, env, appEnvData);
		        if (response == 201) {
		            appEnvData.clear();
		        }
	        } catch (IOException e) {
	            // don't replace if we failed, try again
	            logger.warn("failed to send usage data", e);
	        }
	    });
	}
	
	int post(String app, String env, Object body) throws IOException {
        Request request = new Request.Builder()
                .url(baseUrl + "/" + app + "/" + env)
                .post(RequestBody.create(JSON, json.toJson(body)))
                .build();

        return client.newCall(request).execute().code();
    }
	
	<T> T get(String path, TypeToken<T> type) {
	    try {
    	    Request request = new Request.Builder().url(baseUrl + path).get().build();
    	    return json.fromJson(client.newCall(request).execute().body().string(), type.getType());
	    } catch (IOException e) {
            throw new RuntimeException("unable to get usage data!", e);
        }
    }
	
	private static String path(String... segments) {
	    return "/" + String.join("/", Arrays.asList(segments));
	}
}