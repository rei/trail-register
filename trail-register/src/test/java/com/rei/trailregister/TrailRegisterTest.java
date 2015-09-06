package com.rei.trailregister;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;

import spark.Spark;

public class TrailRegisterTest {
    public static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private Gson json = new Gson();
    private OkHttpClient client = new OkHttpClient();
    private int port;
    private String baseUrl;

    @Before
    public void setup() throws IOException {
        port = findRandomOpenPort();
        baseUrl = "http://localhost:" + port;
        Spark.port(port);
        new TrailRegister(tmp.newFolder("main").toPath(), Collections.emptyList()).run();
        Spark.awaitInitialization();
    }
    
    @Test
    public void integrationTest() throws IOException {
        assertEquals(201, post("/test-app/prod/things/1", ""));
        assertEquals(201, post("/test-app/prod/things/7", ""));
        assertEquals(201, post("/test-app/prod/things", ImmutableMap.of("7", 7, "75", 102)));
        assertEquals(201, post("/test-app/prod/other", ImmutableMap.of("x", 1, "y", 12)));
        
        assertEquals(201, post("/other-app/test/other/x", ""));
        assertEquals(201, post("/other-app/prod/other/x", ""));
        
        assertEquals(201, post("/other-app/prod", ImmutableMap.of("cats", ImmutableMap.of("ninja", 7, "bandit", 102), 
                                                                  "dogs", ImmutableMap.of("dexter", 35, "spot", 1))));
        
        String health = get("/health", new TypeToken<String>(){});
        assertEquals("UP", health);
        
        List<String> apps = get("/", new TypeToken<List<String>>(){});
        assertEquivalant(Arrays.asList("test-app", "other-app"), apps);
        
        List<String> envs = get("/other-app", new TypeToken<List<String>>(){});
        assertEquivalant(Arrays.asList("test", "prod"), envs);
        
        List<String> categories = get("/test-app/prod", new TypeToken<List<String>>(){});
        assertEquivalant(Arrays.asList("things", "other"), categories);
        
        categories = get("/other-app/prod", new TypeToken<List<String>>(){});
        assertEquivalant(Arrays.asList("other", "cats", "dogs"), categories);
        
        Map<String, Integer> usages = get("/test-app/prod/things", new TypeToken<Map<String, Integer>>(){});
        System.out.println(usages);
        assertEquals(3, usages.size());
        assertEquals(8, (int) usages.get("7")); 
        
        usages = get("/other-app/prod/cats", new TypeToken<Map<String, Integer>>(){});
        System.out.println(usages);
        assertEquals(2, usages.size());
        assertEquals(102, (int) usages.get("bandit"));
        
        usages = get("/other-app/prod/cats/bandit?by_date=true&days=7", new TypeToken<Map<String, Integer>>(){});
        System.out.println(usages);
        assertEquals(7, usages.size());
    }

    @Test
    public void clusteringTest() throws IOException, InterruptedException {
    	List<HostAndPort> peers = Arrays.asList(HostAndPort.fromParts("localhost", port));
		ClusteredUsageRepository clusteredRepos = new ClusteredUsageRepository(tmp.newFolder("other").toPath(), UUID.randomUUID(), peers);
		
		for (int i = 0; i < 20; i++) {
			assertEquals(201, post("/test-app/prod/things/x", ""));
		}
		
		clusteredRepos.recordUsages("test-app", "prod", "things", "x");
		
		for (int i = 0; i < 20; i++) {
			int usages = clusteredRepos.getUsages("test-app", "prod", "things", "x", 1);
			assertEquals(21, usages);
		}
		
		get("/_stats", new TypeToken<List<Map<String, Object>>>(){}).forEach(System.out::println);;
    }
    
    @After
    public void cleanup() {
        Spark.stop();
    }

    private void assertEquivalant(List<?> expected, List<?> actual) {
        System.out.println(actual);
        assertEquals(expected.size(), actual.size());
        expected.forEach(e -> assertTrue("should contain '" + e + "'", actual.contains(e)));
    }
    
    private Integer findRandomOpenPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0);) {
            return socket.getLocalPort();
        }
    }

    <T> T get(String path, TypeToken<T> type) throws IOException {
        Request request = new Request.Builder().url(baseUrl + path).get().build();
        return json.fromJson(client.newCall(request).execute().body().string(), type.getType());
    }

    int post(String path, Object body) throws IOException {
        Request request = new Request.Builder()
                .url(baseUrl + path)
                .post(RequestBody.create(JSON, json.toJson(body)))
                .build();

        return client.newCall(request).execute().code();
    }
}
