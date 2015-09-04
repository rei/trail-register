package com.rei.trailregister.client;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.rei.trailregister.TrailRegister;
import com.rei.trailregister.UsageRepository;

import spark.Spark;

public class BatchingTrailRegisterClientTest {
	
	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();
	
	private int port;
    private String baseUrl;

    private UsageRepository backingRepo;
    
    @Before
    public void setup() throws IOException {
        port = findRandomOpenPort();
        baseUrl = "http://localhost:" + port;
        Spark.port(port);
        new TrailRegister(tmp.getRoot().toPath()).run();
        backingRepo = new UsageRepository(tmp.getRoot().toPath());
        Spark.awaitInitialization();
    }
	
	@Test
	public void canSendUsageData() throws InterruptedException, IOException {
		BatchingTrailRegisterClient client = new BatchingTrailRegisterClient(baseUrl, "test-app", "prod", 100, TimeUnit.MILLISECONDS);
		
		for (int i = 0; i < 1000; i++) {
			client.recordUsage("things", "thing" + i % 4);
		}
		
		Thread.sleep(500);
		
		listFiles(tmp.getRoot().toPath());
		
		int usages = backingRepo.getUsages("test-app", "prod", "things", "thing0", 1);
		assertEquals(250, usages);
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