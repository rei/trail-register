package com.rei.trailregister;

import static java.util.stream.Collectors.toList;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;
import com.rei.trailregister.client.TrailRegisterClient;

public class ClusteredFileUsageRepository extends FileUsageRepository {
	private static Logger logger = LoggerFactory.getLogger(ClusteredFileUsageRepository.class);

    private ScheduledExecutorService availabilityCheckExecutor = Executors.newScheduledThreadPool(1); 
    
    private volatile List<Peer> availablePeers = Collections.emptyList();
    private List<Peer> possiblePeers;
	private UUID id;
	private volatile CountDownLatch initialized = new CountDownLatch(1);
	
    public ClusteredFileUsageRepository(Path dataDir, UUID id, List<HostAndPort> peers) {
        super(dataDir);
		this.id = id;
        possiblePeers = peers.stream().map(Peer::new).collect(toList());
        availabilityCheckExecutor.scheduleAtFixedRate(this::checkAvailability, 0, 1, TimeUnit.MINUTES);
    }
    
    @Override
    public Map<String, Integer> getUsagesByDate(UsageKey key, int days, boolean internal) {
    	awaitInitialization();
        Map<String, Integer> localResult = super.getUsagesByDate(key, days, internal);
        if (!internal) {
            availablePeers.parallelStream().forEach(peer -> 
                localResult.putAll(peer.client.getUsagesByDate(key.getApp(), key.getEnv(), key.getCategory(), key.getKey(), days)));
        }
        return localResult;
    }

    @Override
    public int getUsages(UsageKey key, int days, boolean internal) {
    	awaitInitialization();
        int localResult = super.getUsages(key, days, internal);
        return localResult + (!internal ? getUsagesFromPeers(key, days) : 0);
    }

    private int getUsagesFromPeers(UsageKey key, int days) {
        return availablePeers.parallelStream()
                             .mapToInt(peer -> peer.client.getUsages(key.getApp(), key.getEnv(), key.getCategory(), key.getKey(), days))
                             .sum();
    }
    
    void checkAvailability() {
    	List<Peer> peersBefore = availablePeers;
        availablePeers = possiblePeers.stream().filter(Peer::isAvailable).collect(toList());
        if (!peersBefore.equals(availablePeers)) {
        	logger.info("available peers changed to: {}", availablePeers);
        }
        
        initialized.countDown();
    }
    
    private void awaitInitialization() {
    	try {
    		initialized.await();
    	} catch (InterruptedException e) {
    		// nevermind then, don't wait for initialization...
    	}
    }
    
    private class Peer {
    	private HostAndPort host;
    	private TrailRegisterClient client; 
    	
    	public Peer(HostAndPort host) {
    		this.host = host;
    		client = new ClusterAwareTrailRegisterClient(getBaseUrl()); 
		}
    	
    	boolean isAvailable() {
    		try {
    			String pingResult = client.ping();
    			return pingResult != null && !pingResult.equals(id.toString());
    		} catch (RuntimeException e) {
    			return false;
    		}
    	}
    	
    	private String getBaseUrl() {
    		return "http://" + host.getHostText() + ":" + host.getPort();
    	}
    	
    	@Override
    	public String toString() {
    		return host.toString();
    	}
    }
}
