package com.rei.trailregister.client;

import java.util.Map;

public interface TrailRegisterClient {
	void recordUsage(String app, String env, String category, String key);
	int getUsages(String app, String env, String category, String key);
	Map<String, Integer> getUsagesByDate(String app, String env, String category, String key);
    Map<String, Integer> getAllUsages(String app, String env, String category);
    
    int getUsages(String app, String env, String category, String key, int days);
    Map<String, Integer> getUsagesByDate(String app, String env, String category, String key, int days);
    Map<String, Integer> getAllUsages(String app, String env, String category, int days);
}
