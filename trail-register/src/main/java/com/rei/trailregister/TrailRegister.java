package com.rei.trailregister;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static spark.Spark.exception;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.diffplug.common.base.DurianPlugins;
import com.diffplug.common.base.Errors;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import spark.Request;
import spark.Route;
import spark.Spark;

public class TrailRegister {
	private static final String DATA_DIR_VAR = "DATA_DIR";

    private static final String PORT = "PORT";

    private static final String POM_PROPS = "META-INF/maven/com.rei.stats/trail-register/pom.properties";
    
    private static Logger logger = LoggerFactory.getLogger(TrailRegister.class);
    
    public static void main(String[] args) throws IOException {
        DurianPlugins.register(Errors.Plugins.Log.class, error -> {
            logger.error("error!", error);
        });
        
        Optional.ofNullable(System.getenv(PORT)).map(Integer::parseInt).ifPresent(p -> Spark.port(p));
        
        new TrailRegister(Paths.get(Optional.ofNullable(System.getenv(DATA_DIR_VAR)).orElse("/trail-register-data")),
                          ClusterUtils.parseHostAndPorts(System.getenv())).run();
    }
    
    private ScheduledExecutorService compactionExec = Executors.newScheduledThreadPool(1);
    private Gson json = new Gson();
    private UsageRepository repo;
    private UUID id;
    private Map<String, AtomicLong> elapsedTime = new ConcurrentHashMap<>();
    private Map<String, AtomicLong> invocations = new ConcurrentHashMap<>();

    public TrailRegister(Path dataDir, List<HostAndPort> peers) throws IOException {
        if (!Files.exists(dataDir)) {
            Files.createDirectories(dataDir);
        }
        
        id = UUID.randomUUID();
        repo = peers.isEmpty() ? new FileUsageRepository(dataDir) : new ClusteredFileUsageRepository(dataDir, id, peers);
        compactionExec.scheduleWithFixedDelay(repo::runCompaction, 1, 1, TimeUnit.DAYS);
    }
    
    public void run() {
        exception(IllegalArgumentException.class, (e, request, response) -> {
            response.status(400);
            response.body(e.getMessage());
        });
        
    	get("/_ping", (req, res) -> id.toString() );
    	get("/_stats", (req, res) -> elapsedTime.keySet().stream().map(r -> {
			long invocationCount = invocations.get(r).get();
			invocationCount = invocationCount == 0 ? 1 : invocationCount;
			return ImmutableMap.of("req", r, 
								   "total", elapsedTime.get(r).get(), 
									"count", invocationCount,
    								"avg", elapsedTime.get(r).get() / invocationCount);
    		}).collect(toList())
		);
    	
    	get("/health", (req, res) -> repo.getApps() != null ? "UP" : "DOWN");
    	
    	get("/version", (req, res) -> readVersionInfo());
    	
    	get("/", (req, res) -> repo.getApps());
        get("/:app", (req, res) -> repo.getEnvironments(req.params(":app")));
        get("/:app/:env", (req, res) -> repo.getCategories(req.params(":app"), req.params(":env")));
        
        get("/:app/:env/:cat", (req, res) -> {
        	return repo.getKeys(req.params(":app"), req.params(":env"), req.params(":cat")).stream()
        			.collect(toMap(k -> k, 
        					       k -> repo.getUsages(toUsageKey(req, k), days(req), isInternal(req))));
        });
        
        get("/:app/:env/:cat/:key", (req, res) -> {
            if ("true".equals(req.queryParams("by_date"))) {
                return repo.getUsagesByDate(toUsageKey(req), days(req), isInternal(req));
            }
            return repo.getUsages(toUsageKey(req), days(req), isInternal(req));
        });
                
        
        post("/:app/:env", (req, res) -> {
            Map<String, Map<String, Integer>> body = parseJson(req, new TypeToken<Map<String, Map<String, Integer>>>() {});
            body.forEach((category, usages) -> {
                usages.forEach((key, num) -> {
                    repo.recordUsages(new UsageKey(req.params(":app"), req.params(":env"), category, key), num);
                });
            });
            res.status(201);
            return "";
        });
        
        post("/:app/:env/:cat", (req, res) -> {
            Map<String, Integer> body = parseJson(req, new TypeToken<Map<String, Integer>>() {});
            body.forEach((key, num) -> {
                repo.recordUsages(new UsageKey(req.params(":app"), req.params(":env"), req.params(":cat"), key), num);
            });
            res.status(201);
            return "";
        });
        
        post("/:app/:env/:cat/:key", (req, res) -> {
            repo.recordUsages(new UsageKey(req.params(":app"), req.params(":env"), req.params(":cat"), req.params(":key")));
            res.status(201);
            return "";
        });
        
    }

    private boolean isInternal(Request req) {
        return req.headers(ClusterAwareTrailRegisterClient.CLUSTERING_HEADER) != null;
    }

    private UsageKey toUsageKey(Request req) {
        return toUsageKey(req, req.params(":key"));
    }
    
    private UsageKey toUsageKey(Request req, String key) {
        return new UsageKey(req.params(":app"), req.params(":env"), req.params(":cat"), key);
    }
    
    private <T> T parseJson(Request req, TypeToken<T> typeToken) {
        return json.fromJson(req.body(), typeToken.getType());
    }
    
	private static Integer days(Request req) {
		return Optional.ofNullable(req.queryParams("days")).map(Integer::parseInt).orElse(30);
	}
	
	private void get(String path, Route route) {
		Spark.get(path, wrap("GET", path, route), json::toJson);
	}
	
	private void post(String path, Route route) {
		Spark.post(path, wrap("POST", path, route), json::toJson);
	}
	
	private Route wrap(String method, String path, Route route) {
		return (req, res) -> {
			AtomicLong elapsed = elapsedTime.computeIfAbsent(method + " " + path, k -> new AtomicLong());
			AtomicLong counter = invocations.computeIfAbsent(method + " " + path, k -> new AtomicLong());
			long start = System.currentTimeMillis();
			
			Object result = route.handle(req, res);
			
			elapsed.addAndGet(System.currentTimeMillis() - start);
			counter.incrementAndGet();
			
			res.header("Content-Type", "application/json");
			
			return result;
		};
	}
	
	private static Properties readVersionInfo() {
	    InputStream in = ClassLoader.getSystemClassLoader().getResourceAsStream(POM_PROPS);
	    if (in == null) {
	        return new Properties();
	    }
	    Properties props = new Properties();
	    try {
            props.load(in);
        } catch (IOException e) {
            logger.warn("error reading pom properties", e);
            return new Properties();
        }
	    return props;
	}
}