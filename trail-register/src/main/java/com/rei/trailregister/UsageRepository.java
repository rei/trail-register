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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class UsageRepository {
    
    private static final int MIN_COMPACTION_SIZE = 100;
    private static Logger logger = LoggerFactory.getLogger(UsageRepository.class);
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
	
	public UsageRepository(Path basedir) {
		this.basedir = basedir;
	}
	
	public void recordUsages(String app, String env, String category, String key) {
		recordUsages(app, env, category, key, 1, LocalDate.now());
	}
	
	public void recordUsages(String app, String env, String category, String key, int num) {
        recordUsages(app, env, category, key, num, LocalDate.now());
    }
	
	public void recordUsages(String app, String env, String category, String key, LocalDate date) {
		recordUsages(app, env, category, key, 1, date);
	}
	
	public void recordUsages(String app, String env, String category, String key, int num, LocalDate date) {
		checkNotNull(app);
		checkNotNull(env);
		checkNotNull(category);
		checkNotNull(key);
		
		int[] count = new int[] {0};
		
		withLock(compactionLock.readLock(), () -> {
		    withLock(Paths.get(basedir.toString(), app, env, category, key, BASIC_ISO_DATE.format(date)), 
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

    public List<String> getApps() {
		return list();
	}
	
	public List<String> getEnvironments(String app) {
		return list(app);
	}
	
	public List<String> getCategories(String app, String env) {
		return list(app, env);
	}
	
	public List<String> getKeys(String app, String env, String category) {
		return list(app, env, category);
	}
	
	private List<String> list(String... parts) {
		try {
			return Files.list(Paths.get(basedir.toString(), parts))
					.filter(Files::isDirectory)
					.map(Path::getFileName)
					.map(Path::toString)
					.collect(Collectors.toList());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public int getUsages(String app, String env, String category, String key, int days) {
	    Map<String, Integer> compactedData = readCompactedFile(app, env, category, key);
	    
		LocalDate now = LocalDate.now();
		return IntStream.range(0, days)
						.map(delta -> getUsages(app, env, category, key, now.minusDays(delta), compactedData))
						.sum();		
	}
	
	public int getUsages(String app, String env, String category, String key, LocalDate date) {
	    return getUsages(app, env, category, key, date, readCompactedFile(app, env, category, key));
	}
	
	private int getUsages(String app, String env, String category, String key, LocalDate date, Map<String, Integer> compactedData) {
		checkNotNull(app);
		checkNotNull(env);
		checkNotNull(category);
		checkNotNull(key);
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
}
