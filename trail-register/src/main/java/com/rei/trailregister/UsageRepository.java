package com.rei.trailregister;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

public interface UsageRepository {

    void recordUsages(UsageKey key);
    void recordUsages(UsageKey key, int num);
    void recordUsages(UsageKey key, LocalDate date);
    void recordUsages(UsageKey key, int num, LocalDate date);

    List<String> getApps();
    List<String> getEnvironments(String app);
    List<String> getCategories(String app, String env);
    List<String> getKeys(String app, String env, String category);
    Map<String, Integer> getUsagesByDate(UsageKey key, int days);
    int getUsages(UsageKey key, int days);
    
    default void runCompaction() {}
}