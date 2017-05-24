package com.rei.trailregister.client;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Request.Builder;
import com.squareup.okhttp.RequestBody;

abstract class AbstractTrailRegisterClient implements TrailRegisterClient {

    protected static Logger logger = LoggerFactory.getLogger(BatchingTrailRegisterClient.class);
    
    public static final int DEFAULT_DAYS = 30;
    public static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    private Gson json = new Gson();
    private OkHttpClient client = new OkHttpClient();
    protected String baseUrl;

    AbstractTrailRegisterClient(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    @Override
    public long getUsages(String app, String env, String category, String key) {
        return getUsages(app, env, category, key, DEFAULT_DAYS);
    }

    @Override
    public Map<String, Long> getAllUsages(String app, String env, String category) {
        return getAllUsages(app, env, category, DEFAULT_DAYS);
    }

    @Override
    public Map<String, Long> getUsagesByDate(String app, String env, String category, String key) {
        return getUsagesByDate(app, env, category, key, DEFAULT_DAYS);
    }

    @Override
    public long getUsages(String app, String env, String category, String key, int days) {
        return get(path(app, env, category, key) + "?days=" + days, new TypeToken<Long>(){});
    }

    @Override
    public Map<String, Long> getAllUsages(String app, String env, String category, int days) {
        return get(path(app, env, category) + "?days=" + days, new TypeToken<Map<String, Long>>(){});
    }

    @Override
    public Map<String, Long> getUsagesByDate(String app, String env, String category, String key, int days) {
        return get(path(app, env, category, key + "?by_date=true&days=" + days), new TypeToken<Map<String, Long>>(){});
    }

    @Override
    public String ping() {
        return get("/_ping", new TypeToken<String>(){});
    }
    
    @Override
    public List<String> getApps() {
        return get("/", new TypeToken<List<String>>(){});
    }

    @Override
    public List<String> getEnvironments(String app) {
        return get(path(app), new TypeToken<List<String>>(){});
    }

    @Override
    public List<String> getCategories(String app, String env) {
        return get(path(app, env), new TypeToken<List<String>>(){});
    }

    @Override
    public List<String> getKeys(String app, String env, String category) {
        return get(path(app, env, category) + "?keys=true", new TypeToken<List<String>>(){});
    }
    
    protected int post(String app, String env, Object body) throws IOException {
        return post(path(app, env), json.toJson(body));
    }
    
    protected int post(String path, String body) throws IOException {
        Request request = new Request.Builder()
                .url(baseUrl + path)
                .post(RequestBody.create(JSON, body))
                .build();
    
        return client.newCall(request).execute().code();
    }

    protected <T> T get(String path, TypeToken<T> type) {
        try {
    	    Builder request = new Request.Builder().url(baseUrl + path).get();
    	    modifyRequest(request);
    	    return json.fromJson(client.newCall(request.build()).execute().body().string(), type.getType());
        } catch (IOException e) {
            throw new RuntimeException("unable to get usage data!", e);
        }
    }

    static String path(String... segments) {
        return "/" + String.join("/", Arrays.asList(segments));
    }
    
    protected void modifyRequest(Builder b) {}
}