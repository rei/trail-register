package com.rei.trailregister;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDate;
import java.util.Map;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileUsageRepositoryTest {

	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();
	
	private FileUsageRepository repo;
	
	@Before 
	public void setup() {
	    repo = new FileUsageRepository(tmp.getRoot().toPath());
	}
	
	@Test
	public void canReadAndWriteUsages() throws IOException {
		UsageKey usageKey = new UsageKey("app", "env", "tests", "read_write");
        repo.recordUsages(usageKey, 5, LocalDate.now());
		repo.recordUsages(usageKey);
		repo.recordUsages(usageKey, LocalDate.now().minusDays(1));
		repo.recordUsages(usageKey, 4, LocalDate.now().minusDays(2));
		
		assertEquals(6, repo.getUsages(usageKey, 1));
		
		assertEquals(11, repo.getUsages(usageKey, 3));
		
		IntStream.range(0, 365).mapToObj(x -> LocalDate.now().minusDays(x)).forEach(date -> {
		    repo.recordUsages(usageKey, 1, date);    
		});
		
		listFiles(tmp.getRoot().toPath());
		
		repo.runCompaction();
		
		listFiles(tmp.getRoot().toPath());
		
		repo.recordUsages(usageKey);
		repo.recordUsages(usageKey, 2, LocalDate.now().minusDays(2));
		assertEquals(379, repo.getUsages(usageKey, 366));
		
		Map<String, Integer> usagesByDate = repo.getUsagesByDate(usageKey, 366);
        System.out.println(usagesByDate);
        assertEquals(366, usagesByDate.size());
		
	}

	@Test
	public void canReadUsagesForNonExistentKey() {
	    assertEquals(0, repo.getUsages(new UsageKey("app", "env", "tests", "read_write"), 366));
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void requiresNonNullArgs() {
	    repo.recordUsages(new UsageKey(null, "blah", "blah", "<invalid"));
	}
	
	@Test(expected=IllegalArgumentException.class)
    public void requiresSafeArgs() {
        repo.recordUsages(new UsageKey("blah", "blah", "blah", "<invalid"));
    }
	
	@Test(expected=IllegalArgumentException.class)
    public void requiresNonNavigationalArgs() {
        repo.recordUsages(new UsageKey("blah", "blah", "blah", ".."));
    }
	
	@Test(expected=IllegalArgumentException.class)
    public void requiresNonNavigationalArgsToList() {
        repo.getEnvironments("..");
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
