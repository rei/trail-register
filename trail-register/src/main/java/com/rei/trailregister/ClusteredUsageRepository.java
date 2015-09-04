package com.rei.trailregister;

import static java.util.stream.Collectors.toList;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.net.HostAndPort;
import com.rei.trailregister.client.DirectTrailRegisterClient;
import com.rei.trailregister.client.TrailRegisterClient;

public class ClusteredUsageRepository extends UsageRepository {

    private ScheduledExecutorService availabilityCheckExecutor = Executors.newScheduledThreadPool(1); 
    
    private volatile List<TrailRegisterClient> availablePeers = Collections.emptyList();
    
    private List<TrailRegisterClient> possiblePeers;

    public ClusteredUsageRepository(Path dataDir, List<HostAndPort> peers) {
        super(dataDir);
        this.possiblePeers = peers.stream().map(p -> new DirectTrailRegisterClient(getBaseUrl(p))).collect(toList());
        availabilityCheckExecutor.schedule(this::checkAvailability, 1, TimeUnit.MINUTES);
    }
    
    @Override
    public Map<String, Integer> getUsagesByDate(String app, String env, String category, String key, int days) {
        Map<String, Integer> localResult = super.getUsagesByDate(app, env, category, key, days);
        availablePeers.parallelStream().forEach(client -> localResult.putAll(client.getUsagesByDate(app, env, category, key)));
        return localResult;
    }
    
    @Override
    public int getUsages(String app, String env, String category, String key, int days) {
        int localResult = super.getUsages(app, env, category, key, days);
        return localResult + availablePeers.parallelStream()
                                           .mapToInt(client -> client.getUsages(app, env, category, key, days))
                                           .sum();
    }
    
    void checkAvailability() {
        availablePeers = possiblePeers.stream().filter(this::isAvailable).collect(toList());
    }
    
    boolean isAvailable(TrailRegisterClient client) {
        try {
            String pingResult = client.ping();
            return pingResult != null && !pingResult.equals(ClusterUtils.localhost);
        } catch (RuntimeException e) {
            return false;
        }
    }
    
    private static String getBaseUrl(HostAndPort p) {
        return "http://" + p.getHostText() + ":" + p.getPort();
    }
}
