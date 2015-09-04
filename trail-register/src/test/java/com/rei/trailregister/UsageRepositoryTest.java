package com.rei.trailregister;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDate;
import java.util.stream.IntStream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class UsageRepositoryTest {

	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();
	
	@Test
	public void canReadAndWriteUsages() throws IOException {
		UsageRepository repo = new UsageRepository(tmp.getRoot().toPath());
		repo.recordUsages("app", "env", "tests", "read_write", 5, LocalDate.now());
		repo.recordUsages("app", "env", "tests", "read_write");
		repo.recordUsages("app", "env", "tests", "read_write", LocalDate.now().minusDays(1));
		repo.recordUsages("app", "env", "tests", "read_write", 4, LocalDate.now().minusDays(2));
		
		assertEquals(6, repo.getUsages("app", "env", "tests", "read_write", LocalDate.now()));
		
		assertEquals(11, repo.getUsages("app", "env", "tests", "read_write", 3));
		
		IntStream.range(0, 365).mapToObj(x -> LocalDate.now().minusDays(x)).forEach(date -> {
		    repo.recordUsages("app", "env", "tests", "read_write", 1, date);    
		});
		
		listFiles(tmp.getRoot().toPath());
		
		repo.runCompaction();
		
		listFiles(tmp.getRoot().toPath());
		
		repo.recordUsages("app", "env", "tests", "read_write");
		repo.recordUsages("app", "env", "tests", "read_write", 2, LocalDate.now().minusDays(2));
		assertEquals(379, repo.getUsages("app", "env", "tests", "read_write", 366));
		
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
