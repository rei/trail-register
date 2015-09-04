package com.rei.trailregister.client;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.rei.trailregister.TrailRegister;

import spark.Spark;

public class BatchingTrailRegisterClientTest {
	
	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();
	
	private int port;
    private String baseUrl;

    
    @Before
    public void setup() throws IOException {
        port = findRandomOpenPort();
        baseUrl = "http://localhost:" + port;
        Spark.port(port);
        new TrailRegister(tmp.getRoot().toPath()).run();
        Spark.awaitInitialization();
    }
    
    @After
    public void cleanup() {
        Spark.stop();
    }
	
	@Test
	public void canSendUsageData() throws InterruptedException, IOException {
		BatchingTrailRegisterClient client = new BatchingTrailRegisterClient(baseUrl, 100, TimeUnit.MILLISECONDS);
		
		for (int i = 0; i < 1000; i++) {
			client.recordUsage("test-app", "prod", "things", "thing" + i % 4);
		}
		
		Thread.sleep(500);
		
		listFiles(tmp.getRoot().toPath());
		
		int usages = client.getUsages("test-app", "prod", "things", "thing0");
		assertEquals(250, usages);
		
		usages = client.getUsages("test-app", "prod", "things", "thing0", 1);
        assertEquals(250, usages);
		
		Map<String, Integer> usagesByDate = client.getUsagesByDate("test-app", "prod", "things", "thing0");
		System.out.println(usagesByDate);
        assertEquals(BatchingTrailRegisterClient.DEFAULT_DAYS, usagesByDate.size()); 
        assertEquals(250, (int) usagesByDate.values().stream().reduce(Integer::sum).orElse(0));
        
        usagesByDate = client.getUsagesByDate("test-app", "prod", "things", "thing0", 10);
        assertEquals(10, usagesByDate.size()); 
	}
	
    private Integer findRandomOpenPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0);) {
            return socket.getLocalPort();
        }
    }
    
	void listFiles(Path path) throws IOException {
	    Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
	        @Override
	        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
	             if(!attrs.isDirectory()){
	                  System.out.println(file + " " + Files.size(file));
	             }
	             return FileVisitResult.CONTINUE;
	         }
        });
	}
}