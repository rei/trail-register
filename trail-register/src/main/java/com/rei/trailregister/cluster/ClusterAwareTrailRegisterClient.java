package com.rei.trailregister.cluster;

import com.rei.trailregister.client.DirectTrailRegisterClient;
import com.squareup.okhttp.Request.Builder;

public class ClusterAwareTrailRegisterClient extends DirectTrailRegisterClient {

    public static final String CLUSTERING_HEADER = "X-From-Peer";

    public ClusterAwareTrailRegisterClient(String baseUrl) {
        super(baseUrl);
    }

    @Override
    protected void modifyRequest(Builder b) {
        b.addHeader(CLUSTERING_HEADER, "true");
    }
}
