package com.rei.trailregister;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.rei.trailregister.client.TrailRegisterClient;

public class ReadOnlyClientUsageRepository implements UsageRepository {

    private TrailRegisterClient client;

    public ReadOnlyClientUsageRepository(TrailRegisterClient client) {
        this.client = client;
    }

    @Override
    public void recordUsages(UsageKey key, int num, LocalDate date) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getApps() {
        return client.getApps();
    }

    @Override
    public List<String> getEnvironments(String app) {
        return client.getEnvironments(app);
    }

    @Override
    public List<String> getCategories(String app, String env) {
        return client.getCategories(app, env);
    }

    @Override
    public List<String> getKeys(String app, String env, String category) {
//        return client.getKeys(app, env, category); // uncomment when using fixed version of trail register
        return new ArrayList<>(client.getAllUsages(app, env, category).keySet());
    }

    @Override
    public Map<String, Long> getUsagesByDate(UsageKey key, int days) {
        return client.getUsagesByDate(key.getApp(), key.getEnv(), key.getCategory(), key.getKey(), days);
    }

    @Override
    public long getUsages(UsageKey key, int days) {
        return client.getUsages(key.getApp(), key.getEnv(), key.getCategory(), key.getKey(), days);
    }
}
