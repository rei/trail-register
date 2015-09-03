package com.rei.trailregister;

import static org.junit.Assert.assertEquals;

import java.time.LocalDate;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class UsageRepositoryTest {

	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();
	
	@Test
	public void canReadAndWriteUsages() {
		UsageRepository repo = new UsageRepository(tmp.getRoot().toPath());
		repo.recordUsages("app", "env", "tests", "read_write", 5, LocalDate.now());
		repo.recordUsages("app", "env", "tests", "read_write");
		repo.recordUsages("app", "env", "tests", "read_write", LocalDate.now().minusDays(1));
		repo.recordUsages("app", "env", "tests", "read_write", 4, LocalDate.now().minusDays(2));
		
		assertEquals(6, repo.getUsages("app", "env", "tests", "read_write", LocalDate.now()));
		
		assertEquals(11, repo.getUsages("app", "env", "tests", "read_write", 3));
	}

}
