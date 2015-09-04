package com.rei.trailregister.client;

import java.io.IOException;

public class DirectTrailRegisterClient extends AbstractTrailRegisterClient implements TrailRegisterClient {
	
	public DirectTrailRegisterClient(String baseUrl) {
		super(baseUrl);
	}
	
	@Override
	public void recordUsage(String app, String env, String category, String key) {
	    try {
            post(path(app, env, category, key), "");
        } catch (IOException e) {
            throw new RuntimeException("unable to send usage data!", e);
        }
	}
}