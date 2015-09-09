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

public class UsageRepositoryTest {

	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();
	
	private UsageRepository repo;
	
	@Before 
	public void setup() {
	    repo = new UsageRepository(tmp.getRoot().toPath());
	}
	
	@Test
	public void canReadAndWriteUsages() throws IOException {
		repo.recordUsages("app", "env", "tests", "read_write", 5, LocalDate.now());
		repo.recordUsages("app", "env", "tests", "read_write");
		repo.recordUsages("app", "env", "tests", "read_write", LocalDate.now().minusDays(1));
		repo.recordUsages("app", "env", "tests", "read_write", 4, LocalDate.now().minusDays(2));
		
		assertEquals(6, repo.getUsages("app", "env", "tests", "read_write", LocalDate.now()));
		
		assertEquals(11, repo.getUsages(new GetUsagesRequest("app", "env", "tests", "read_write", 3, false)));
		
		IntStream.range(0, 365).mapToObj(x -> LocalDate.now().minusDays(x)).forEach(date -> {
		    repo.recordUsages("app", "env", "tests", "read_write", 1, date);    
		});
		
		listFiles(tmp.getRoot().toPath());
		
		repo.runCompaction();
		
		listFiles(tmp.getRoot().toPath());
		
		repo.recordUsages("app", "env", "tests", "read_write");
		repo.recordUsages("app", "env", "tests", "read_write", 2, LocalDate.now().minusDays(2));
		assertEquals(379, repo.getUsages(new GetUsagesRequest("app", "env", "tests", "read_write", 366, false)));
		
		Map<String, Integer> usagesByDate = repo.getUsagesByDate(new GetUsagesRequest("app", "env", "tests", "read_write", 366, false));
        System.out.println(usagesByDate);
        assertEquals(366, usagesByDate.size());
		
	}

	@Test
	public void canReadUsagesForNonExistentKey() {
	    assertEquals(0, repo.getUsages(new GetUsagesRequest("app", "env", "tests", "read_write", 366, false)));
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void requiresNonNullArgs() {
	    repo.recordUsages(null, "blah", "blah", "<invalid");
	}
	
	@Test(expected=IllegalArgumentException.class)
    public void requiresSafeArgs() {
        repo.recordUsages("blah", "blah", "blah", "<invalid");
    }
	
	@Test(expected=IllegalArgumentException.class)
    public void requiresNonNavigationalArgs() {
        repo.recordUsages("blah", "blah", "blah", "..");
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
