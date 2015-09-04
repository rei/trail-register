package com.rei.trailregister;

import static spark.Spark.before;
import static spark.Spark.get;
import static spark.Spark.post;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.diffplug.common.base.DurianPlugins;
import com.diffplug.common.base.Errors;
import com.google.common.net.HostAndPort;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import spark.Request;

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

    public TrailRegister(Path dataDir, List<HostAndPort> peers) throws IOException {
        if (!Files.exists(dataDir)) {
            Files.createDirectories(dataDir);
        }
        
        repo = peers.isEmpty() ? new UsageRepository(dataDir) : new ClusteredUsageRepository(dataDir, peers);
        compactionExec.scheduleWithFixedDelay(repo::runCompaction, 1, 1, TimeUnit.DAYS);
    }
    
    public void run() {
    	before("/*", "application/json", (req, res) -> res.header("Content-Type", "application/json"));
        
    	get("/", (req, res) -> repo.getApps(), json::toJson);
        get("/:app", (req, res) -> repo.getEnvironments(req.params(":app")), json::toJson);
        get("/:app/:env", (req, res) -> repo.getCategories(req.params(":app"), req.params(":env")), json::toJson);
        
        get("/:app/:env/:cat", (req, res) -> {
        	return repo.getKeys(req.params(":app"), req.params(":env"), req.params(":cat")).stream()
        			.collect(Collectors.toMap(k -> k, 
        					 k -> repo.getUsages(req.params(":app"), req.params(":env"), req.params(":cat"), k, days(req))));
        }, json::toJson);
        
        get("/:app/:env/:cat/:key", (req, res) -> 
            Optional.ofNullable(req.queryParams("by_date")).map(p -> p.equals("true")).orElse(false)  
                    ? repo.getUsagesByDate(req.params(":app"), req.params(":env"), req.params(":cat"), req.params(":key"), days(req)) 
                    : repo.getUsages(req.params(":app"), req.params(":env"), req.params(":cat"), req.params(":key"), days(req)),
                    
            json::toJson);
        
        post("/:app/:env", (req, res) -> {
            Map<String, Map<String, Integer>> body = parseJson(req, new TypeToken<Map<String, Map<String, Integer>>>() {});
            body.forEach((category, usages) -> {
                usages.forEach((key, num) -> {
                    repo.recordUsages(req.params(":app"), req.params(":env"), category, key, num);
                });
            });
            res.status(201);
            return "";
        }, json::toJson);
        
        post("/:app/:env/:cat", (req, res) -> {
            Map<String, Integer> body = parseJson(req, new TypeToken<Map<String, Integer>>() {});
            body.forEach((key, num) -> {
                repo.recordUsages(req.params(":app"), req.params(":env"), req.params(":cat"), key, num);
            });
            res.status(201);
            return "";
        }, json::toJson);
        
        post("/:app/:env/:cat/:key", (req, res) -> {
            repo.recordUsages(req.params(":app"), req.params(":env"), req.params(":cat"), req.params(":key"));
            res.status(201);
            return "";
        }, json::toJson);
        
        get("/_ping", (req, res) -> ClusterUtils.localhost );
    }

    private <T> T parseJson(Request req, TypeToken<T> typeToken) {
        return json.fromJson(req.body(), typeToken.getType());
    }
    
	private static Integer days(Request req) {
		return Optional.ofNullable(req.queryParams("days")).map(Integer::parseInt).orElse(30);
	}
}