package com.rei.trailregister;

import static spark.Spark.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.gson.Gson;

import spark.Request;

public class TrailRegister {
    public static void main(String[] args) throws IOException {
    	Gson json = new Gson();
    	Path dataDir = Paths.get(Optional.ofNullable(System.getenv("DATA_DIR")).orElse("/trail-register-data"));
    	if (!Files.exists(dataDir)) {
    		Files.createDirectories(dataDir);
    	}
    	UsageRepository repo = new UsageRepository(dataDir);
    	
    	before("/*", "application/json", (req, res) -> res.header("Content-Type", "application/json"));
        
    	get("/", (req, res) -> repo.getApps(), json::toJson);
        get("/:app", (req, res) -> repo.getEnvironments(req.params(":app")), json::toJson);
        get("/:app/:env", (req, res) -> repo.getCategories(req.params(":app"), req.params(":env")), json::toJson);
        get("/:app/:env/:cat", (req, res) -> repo.getKeys(req.params(":app"), req.params(":env"), req.params(":cat")), json::toJson);
        
        get("/:app/:env/:cat/_all", (req, res) -> {
        	return repo.getKeys(req.params(":app"), req.params(":env"), req.params(":cat")).stream()
        			.collect(Collectors.toMap(k -> k, 
        					 k -> repo.getUsages(req.params(":app"), req.params(":env"), req.params(":cat"), k, days(req))));
        }, json::toJson);
        
        get("/:app/:env/:cat/:key", (req, res) -> repo.getUsages(req.params(":app"), req.params(":env"), req.params(":cat"), 
        		req.params(":key"), days(req)), json::toJson);
        
        post("/:app/:env/:cat/:key", (req, res) -> {
        	repo.recordUsages(req.params(":app"), req.params(":env"), req.params(":cat"), req.params(":key"));
        	res.status(201);
        	return "";
        }, json::toJson);
    }

	private static Integer days(Request req) {
		return Optional.ofNullable(req.queryParams("days")).map(Integer::parseInt).orElse(30);
	}
}