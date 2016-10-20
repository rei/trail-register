package com.rei.trailregister.client;

import java.util.List;
import java.util.Map;

public interface TrailRegisterClient {
	void recordUsage(String app, String env, String category, String key);
	long getUsages(String app, String env, String category, String key);
	Map<String, Long> getUsagesByDate(String app, String env, String category, String key);
    Map<String, Long> getAllUsages(String app, String env, String category);
    
    
    long getUsages(String app, String env, String category, String key, int days);
    Map<String, Long> getUsagesByDate(String app, String env, String category, String key, int days);
    Map<String, Long> getAllUsages(String app, String env, String category, int days);

    String ping();
    List<String> getApps();
    List<String> getEnvironments(String app);
    List<String> getCategories(String app, String env);
    List<String> getKeys(String app, String env, String category);
}
