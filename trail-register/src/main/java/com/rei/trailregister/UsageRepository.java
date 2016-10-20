package com.rei.trailregister;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

public interface UsageRepository {

    default void recordUsages(UsageKey key) {
        recordUsages(key, 1);
    }
    
    default void recordUsages(UsageKey key, int num) {
        recordUsages(key, num, LocalDate.now());
    }
    
    default void recordUsages(UsageKey key, LocalDate date) {
        recordUsages(key, 1, date);
    }
    
    void recordUsages(UsageKey key, int num, LocalDate date);

    List<String> getApps();
    List<String> getEnvironments(String app);
    List<String> getCategories(String app, String env);
    List<String> getKeys(String app, String env, String category);
    Map<String, Long> getUsagesByDate(UsageKey key, int days);
    long getUsages(UsageKey key, int days);
    
    default void runCompaction() {}
}