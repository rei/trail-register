package com.rei.trailregister;

class GetUsagesRequest {
    String app;
    String env;
    String category;
    String key;
    int days;
    boolean fromPeer;

    GetUsagesRequest(String app, String env, String category, String key, int days, boolean clusterReq) {
        this.app = app;
        this.env = env;
        this.category = category;
        this.key = key;
        this.days = days;
        this.fromPeer = clusterReq;
    }
}