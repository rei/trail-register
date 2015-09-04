package com.rei.trailregister.client;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BatchingTrailRegisterClient extends AbstractTrailRegisterClient {
	// yes, holy crap, 3 levels...
	private ConcurrentMap<String, ConcurrentMap<String, ConcurrentMap<String, Integer>>> data = new ConcurrentHashMap<>();

	private ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
	
	public BatchingTrailRegisterClient(String baseUrl, long interval, TimeUnit unit) {
		super(baseUrl);
		executor.scheduleWithFixedDelay(this::sendBatch, interval, interval, unit);
	}
	
	@Override
	public void recordUsage(String app, String env, String category, String key) {
	    String appEnvKey = app + "|" + env;
	    
		data.compute(appEnvKey, (k, v) -> v == null ? new ConcurrentHashMap<>() : v)
		    .compute(category, (k, v) -> v == null ? new ConcurrentHashMap<>() : v)
			.compute(key, (k, v) -> v == null ? 1 : v + 1);
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
}