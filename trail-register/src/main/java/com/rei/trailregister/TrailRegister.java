package com.rei.trailregister;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    
    private static Logger logger = LoggerFactory.getLogger(TrailRegister.class);
    
    public static void main(String[] args) throws IOException {
        DurianPlugins.register(Errors.Plugins.Log.class, error -> {
            logger.error("error!", error);
        });
        
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
        repo = peers.isEmpty() ? new UsageRepository(dataDir) : new ClusteredUsageRepository(dataDir, id, peers);
        compactionExec.scheduleWithFixedDelay(repo::runCompaction, 1, 1, TimeUnit.DAYS);
    }
    
    public void run() {
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
    	
    	get("/", (req, res) -> repo.getApps());
        get("/:app", (req, res) -> repo.getEnvironments(req.params(":app")));
        get("/:app/:env", (req, res) -> repo.getCategories(req.params(":app"), req.params(":env")));
        
        get("/:app/:env/:cat", (req, res) -> {
        	return repo.getKeys(req.params(":app"), req.params(":env"), req.params(":cat")).stream()
        			.collect(toMap(k -> k, 
        					 k -> repo.getUsages(req.params(":app"), req.params(":env"), req.params(":cat"), k, days(req))));
        });
        
        get("/:app/:env/:cat/:key", (req, res) -> 
            Optional.ofNullable(req.queryParams("by_date")).map(p -> p.equals("true")).orElse(false)  
                    ? repo.getUsagesByDate(req.params(":app"), req.params(":env"), req.params(":cat"), req.params(":key"), days(req)) 
                    : repo.getUsages(req.params(":app"), req.params(":env"), req.params(":cat"), req.params(":key"), days(req)));
        
        post("/:app/:env", (req, res) -> {
            Map<String, Map<String, Integer>> body = parseJson(req, new TypeToken<Map<String, Map<String, Integer>>>() {});
            body.forEach((category, usages) -> {
                usages.forEach((key, num) -> {
                    repo.recordUsages(req.params(":app"), req.params(":env"), category, key, num);
                });
            });
            res.status(201);
            return "";
        });
        
        post("/:app/:env/:cat", (req, res) -> {
            Map<String, Integer> body = parseJson(req, new TypeToken<Map<String, Integer>>() {});
            body.forEach((key, num) -> {
                repo.recordUsages(req.params(":app"), req.params(":env"), req.params(":cat"), key, num);
            });
            res.status(201);
            return "";
        });
        
        post("/:app/:env/:cat/:key", (req, res) -> {
            repo.recordUsages(req.params(":app"), req.params(":env"), req.params(":cat"), req.params(":key"));
            res.status(201);
            return "";
        });
        
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
}