package com.rei.trailregister;

import static java.time.format.DateTimeFormatter.BASIC_ISO_DATE;

import java.sql.Driver;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.commons.dbcp2.BasicDataSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.util.IntegerColumnMapper;
import org.skife.jdbi.v2.util.StringColumnMapper;

public class DatabaseUsageRepository implements UsageRepository {

    private static final int MAX_CONNECTION = 100;
    
    private static final String INSERT = 
            "insert into usages (app, env, category, key, date, num) values (?, ?, ?, ?, ?, ?)";
    
    private static final String SELECT_BY_DATE = 
            "select num from usages where app = ? and env = ? and category = ? and key = ? and date = ?";
    
    private static final String UPDATE = 
            "update usages set num = num + ? where app = ? and env = ? and category = ? and key = ? and date = ?";
    
    private DBI dbi;

    public DatabaseUsageRepository(String url, String user, String pass, Supplier<Driver> driverProvider) {
        BasicDataSource ds = new BasicDataSource();
        ds.setUrl(url);
        ds.setUsername(user);
        ds.setPassword(pass);
        ds.setDriver(driverProvider.get());
        ds.setMaxTotal(MAX_CONNECTION);
        
        dbi = new DBI(ds);
    }

    @Override
    public void recordUsages(UsageKey key, int num, LocalDate date) {
        dbi.withHandle(h -> {
            List<Map<String, Object>> result = h.select(SELECT_BY_DATE, key.getApp(), key.getEnv(), key.getCategory(), key.getKey(), date.toEpochDay());
            if (result.isEmpty()) {
                h.insert(INSERT, key.getApp(), key.getEnv(), key.getCategory(), key.getKey(), date.toEpochDay(), num);
            } else {
                h.update(UPDATE, num, key.getApp(), key.getEnv(), key.getCategory(), key.getKey(), date.toEpochDay());
            }
            
            return null;
        });
    }

    @Override
    public List<String> getApps() {
        return dbi.withHandle(h -> {
            return h.createQuery("select distinct app from usages").map(StringColumnMapper.INSTANCE).list();
        });
    }

    @Override
    public List<String> getEnvironments(String app) {
        return dbi.withHandle(h -> {
            return h.createQuery("select distinct env from usages where app = ?").bind(0, app).map(StringColumnMapper.INSTANCE).list();
        });
    }

    @Override
    public List<String> getCategories(String app, String env) {
        return dbi.withHandle(h -> {
            return h.createQuery("select distinct category from usages where app = ? and env = ?")
                    .bind(0, app)
                    .bind(1, env)
                    .map(StringColumnMapper.INSTANCE)
                    .list();
        });
    }

    @Override
    public List<String> getKeys(String app, String env, String category) {
        return dbi.withHandle(h -> {
            return h.createQuery("select distinct key from usages where app = ? and env = ? and category = ?")
                    .bind(0, app)
                    .bind(1, env)
                    .bind(2, category)
                    .map(StringColumnMapper.INSTANCE)
                    .list();
        });
    }

    @Override
    public Map<String, Integer> getUsagesByDate(UsageKey key, int days) {
        return dbi.withHandle(h -> {
            return h.createQuery("select date, num from usages where app = ? and env = ? and category = ? and key = ? and date > ?")
                    .bind(0, key.getApp())
                    .bind(1, key.getEnv())
                    .bind(2, key.getCategory())
                    .bind(3, key.getKey())
                    .bind(4, LocalDate.now().minusDays(days).toEpochDay())
                    .fold(new HashMap<>(), (m, rs, ctx) -> {
                        m.put(LocalDate.ofEpochDay(rs.getLong(1)).format(BASIC_ISO_DATE), rs.getInt(2));
                        return m;
                    });
            
        });
    }

    @Override
    public int getUsages(UsageKey key, int days) {
        return dbi.withHandle(h -> {
            return h.createQuery("select sum(num) from usages where app = ? and env = ? and category = ? and key = ? and date > ?")
                    .bind(0, key.getApp())
                    .bind(1, key.getEnv())
                    .bind(2, key.getCategory())
                    .bind(3, key.getKey())
                    .bind(4, LocalDate.now().minusDays(days).toEpochDay())
                    .map(IntegerColumnMapper.PRIMITIVE)
                    .first();
        });
    }
    
    DBI getDbi() {
        return dbi;
    }
}
