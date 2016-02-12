package com.rei.trailregister;

public class UsageKey {
    private String app;
    private String env;
    private String category;
    private String key;

    public UsageKey() {
    }

    public UsageKey(String app, String env, String category, String key) {
        this.app = app;
        this.env = env;
        this.category = category;
        this.key = key;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public String getEnv() {
        return env;
    }

    public void setEnv(String env) {
        this.env = env;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
    
    @Override
    public String toString() {
        return String.format("%s/%s/%s/%s", app, env, category, key);
    }
}
