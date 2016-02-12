package com.rei.trailregister;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.time.format.DateTimeFormatter.BASIC_ISO_DATE;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.diffplug.common.base.Errors;
import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class FileUsageRepository implements UsageRepository {
    private static final String INVALID_CHARS = ":*?\"<>|/\\";
    private static final int MIN_COMPACTION_SIZE = 100;
    private static Logger logger = LoggerFactory.getLogger(FileUsageRepository.class);
	private static final String COMPACTED_FILE = "_data";

    private Path basedir;

	private LoadingCache<String, Lock> locks = CacheBuilder.newBuilder()
														 .expireAfterAccess(1, TimeUnit.MINUTES)
														 .build(new CacheLoader<String, Lock>() {
		@Override
		public Lock load(String p) throws Exception {
			return new ReentrantLock();
		}
	});
	
	private ReadWriteLock compactionLock = new ReentrantReadWriteLock(false);
	
	public FileUsageRepository(Path basedir) {
		this.basedir = basedir;
	}
	
	@Override
    public void recordUsages(UsageKey key) {
		recordUsages(key, 1, LocalDate.now());
	}
	
	@Override
    public void recordUsages(UsageKey key, int num) {
        recordUsages(key, num, LocalDate.now());
    }
	
	@Override
    public void recordUsages(UsageKey key, LocalDate date) {
		recordUsages(key, 1, date);
	}
	
	@Override
    public void recordUsages(UsageKey key, int num, LocalDate date) {
		checkArgument("app", key.getApp());
		checkArgument("env", key.getEnv());
		checkArgument("category", key.getCategory());
		checkArgument("key", key.getKey());
		
		int[] count = new int[] {0};
		
		withLock(compactionLock.readLock(), () -> {
		    withLock(Paths.get(basedir.toString(), key.getApp(), key.getEnv(), key.getCategory(), key.getKey(), BASIC_ISO_DATE.format(date)), 
		             Errors.rethrow().wrap(dateFile -> {
		             
        			if (!Files.exists(dateFile)) {
        				Files.createDirectories(dateFile.getParent());
        			} else {
        				count[0] = readDataFile(dateFile);
        			}
        			Files.write(dateFile, String.valueOf(count[0]+num).getBytes());
    		}));
		});
	}

    @Override
    public List<String> getApps() {
		return list();
	}
	
	@Override
    public List<String> getEnvironments(String app) {
		return list(app);
	}
	
	@Override
    public List<String> getCategories(String app, String env) {
		return list(app, env);
	}
	
	@Override
    public List<String> getKeys(String app, String env, String category) {
		return list(app, env, category);
	}
	
	private List<String> list(String... parts) {
		try {
			Path path = Paths.get(basedir.toString(), parts);
			Preconditions.checkArgument(!path.toString().contains(".."), "may not contain '..' or '.'!");
			
			if (!Files.exists(path)) {
				return Collections.emptyList();
			}
			return Files.list(path)
					.filter(Files::isDirectory)
					.map(Path::getFileName)
					.map(Path::toString)
					.collect(Collectors.toList());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
    public Map<String, Integer> getUsagesByDate(UsageKey key, int days) {
	    Map<String, Integer> compactedData = readCompactedFile(key.getApp(), key.getEnv(), key.getCategory(), key.getKey());
        
        LocalDate now = LocalDate.now();
        return new TreeMap<>(IntStream.range(0, days)
                                      .mapToObj(delta -> now.minusDays(delta))
                                      .collect(toMap(BASIC_ISO_DATE::format, 
                                                     date -> getUsages(key.getApp(), key.getEnv(), key.getCategory(), 
                                                                       key.getKey(), date, compactedData))));
	}
	
	@Override
    public int getUsages(UsageKey key, int days) {
	    Map<String, Integer> compactedData = readCompactedFile(key.getApp(), key.getEnv(), key.getCategory(), key.getKey());
	    
		LocalDate now = LocalDate.now();
		return IntStream.range(0, days)
						.map(delta -> getUsages(key.getApp(), key.getEnv(), key.getCategory(), 
                                                key.getKey(), now.minusDays(delta), compactedData))
						.sum();		
	}
	
	protected int getUsages(String app, String env, String category, String key, LocalDate date, Map<String, Integer> compactedData) {
		checkArgument("app", app);
		checkArgument("env", env);
		checkArgument("category", category);
		checkArgument("key", key);
		checkNotNull(date);
		
		String dateString = BASIC_ISO_DATE.format(date);
        Path dateFile = Paths.get(basedir.toString(), app, env, category, key, dateString);
		
		int uncompactedNum = Files.exists(dateFile) ? readDataFile(dateFile) : 0;
		return uncompactedNum + compactedData.computeIfAbsent(dateString, k -> 0);
	}
	
	public void runCompaction() {
	    getApps().forEach(app -> {
    	    getEnvironments(app).forEach(env -> {
    	        getCategories(app, env).parallelStream().forEach(cat -> {
    	            getKeys(app, env, cat).forEach(Errors.rethrow().wrap((String key) -> runCompaction(app, env, cat, key)));
    	        });
    	    });
	    });
	}

    private void runCompaction(String app, String env, String category, String key) throws IOException {
        
        if (getDateFiles(app, env, category, key).size() < MIN_COMPACTION_SIZE) { return; }
        
        withLock(compactionLock.writeLock(), Errors.rethrow().wrap(() -> {
            List<Path> dateFiles = getDateFiles(app, env, category, key);
            if (dateFiles.size() < MIN_COMPACTION_SIZE) { return; }
            
            logger.info("running compaction on {}/{}/{}/{}", app, env, category, key);
            
            dateFiles.forEach(withLock(Errors.rethrow().wrap(dateFile -> {
                 Map<String, Integer> data = readCompactedFile(app, env, category, key);
                 
                 data.merge(dateFile.getFileName().toString(), readDataFile(dateFile), Integer::sum);
                 
                 writeCompactedFile(compactionFile(app, env, category, key), data);
                 Files.delete(dateFile);
            })));
        }));
    }

    private List<Path> getDateFiles(String app, String env, String category, String key) throws IOException {
        return Files.list(Paths.get(basedir.toString(), app, env, category, key))
             .filter(p -> !p.getFileName().toString().equals(COMPACTED_FILE)).collect(toList());
    }
    
    private Map<String, Integer> readCompactedFile(String app, String env, String category, String key) {
        try {
            Path file = compactionFile(app, env, category, key);
            if (!Files.exists(file)) {
                return new LinkedHashMap<>();
            }
            return Files.readAllLines(file).stream()
                    .map(l -> l.split(" "))
                    .collect(toMap(parts -> parts[0], parts -> Integer.parseInt(parts[1])));
        } catch (NumberFormatException | IOException e) {
            logger.error("failed to read compacted file", e);
            throw new IllegalStateException("unable to read compacted file!", e);
        }
    }
    
    private void writeCompactedFile(Path file, Map<String, Integer> data) {
        try {
            Files.write(file, data.entrySet().stream().map(e -> e.getKey() + " " + e.getValue()).collect(toList()));
        } catch (NumberFormatException | IOException e) {
            logger.error("failed to write compacted file", e);
            throw new IllegalStateException("unable to write compacted file!", e);
        }
    }
    
    private int readDataFile(Path dateFile) {
        try {
            return Integer.parseInt(new String(Files.readAllBytes(dateFile)));
        } catch (IOException e) {
            logger.error("failed to read data file", e);
            throw new RuntimeException(e);
        }
    }
    
    private Path compactionFile(String app, String env, String category, String key) {
        return Paths.get(basedir.toString(), app, env, category, key, COMPACTED_FILE);
    }
    
    private Consumer<Path> withLock(Consumer<Path> work) {
        return path -> {
            Lock lock = locks.getUnchecked(path.toString());
            try {
                lock.lock();
                work.accept(path);
            } finally {
                lock.unlock();
            }
        };
    }
    
    private void withLock(Path p, Consumer<Path> work) {
        withLock(f -> work.accept(f)).accept(p);
    }
    
    private void withLock(Lock lock, Runnable work) {
        try {
            lock.lock();
            work.run();
        } finally {
            lock.unlock();
        }
    }
    
    private void checkArgument(String name, String value) {
        Preconditions.checkArgument(value != null, "{0} may not be null", name);
        Preconditions.checkArgument(CharMatcher.anyOf(INVALID_CHARS).matchesNoneOf(value), "%s may not contain %s", name, INVALID_CHARS);
        Preconditions.checkArgument(!value.equals("..") && !value.equals("."), "%s must not equal '..' or '.'", name);
    }
}
