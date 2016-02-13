package com.rei.trailregister;

import static org.junit.Assert.assertEquals;

import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import org.h2.Driver;
import org.junit.Before;
import org.junit.Test;

public class DatabaseUsageRepositoryTest {

    public static final String URL = "jdbc:h2:mem:test" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1";
    private DatabaseUsageRepository repo = new DatabaseUsageRepository(URL, "sa", "sa", Driver::new);
    
    @Before
    public void setup() {
        repo.getDbi().withHandle(h -> {
           h.execute("create table usages (app varchar(20), env varchar(20), category varchar(30), key varchar(100), date int, num int)"); 
           return null; 
        });
    }
    
    @Test
    public void testRecordUsages() {
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
        
        repo.recordUsages(usageKey);
        repo.recordUsages(usageKey, 2, LocalDate.now().minusDays(2));
        assertEquals(379, repo.getUsages(usageKey, 366));
        
        Map<String, Integer> usagesByDate = repo.getUsagesByDate(usageKey, 366);
        System.out.println(usagesByDate);
        assertEquals(365, usagesByDate.size());
    }

}
