package com.rei.trailregister;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.time.format.DateTimeFormatter.BASIC_ISO_DATE;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class UsageRepository {
	private Path basedir;

	private LoadingCache<Path, Lock> locks = CacheBuilder.newBuilder()
														 .expireAfterAccess(1, TimeUnit.MINUTES)
														 .build(new CacheLoader<Path, Lock>() {
		@Override
		public Lock load(Path p) throws Exception {
			return new ReentrantLock();
		}
	});
	
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
		
		Path dateFile = Paths.get(basedir.toString(), app, env, category, key, BASIC_ISO_DATE.format(date));
		Lock lock = locks.getUnchecked(dateFile);
		try {
			lock.lock();
			try {
				int count = 0;
				if (!Files.exists(dateFile)) {
					Files.createDirectories(dateFile.getParent());
				} else {
					count = Files.readAllBytes(dateFile)[0];
				}
				Files.write(dateFile, new byte[] { (byte) (count+num) });
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		} finally {
			lock.unlock();
		}
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
		LocalDate now = LocalDate.now();
		return IntStream.range(0, days)
						.map(delta -> getUsages(app, env, category, key, now.minusDays(delta)))
						.sum();		
	}
	
	public int getUsages(String app, String env, String category, String key, LocalDate date) {
		checkNotNull(app);
		checkNotNull(env);
		checkNotNull(category);
		checkNotNull(key);
		checkNotNull(date);
		
		Path dateFile = Paths.get(basedir.toString(), app, env, category, key, BASIC_ISO_DATE.format(date));
		
		if (Files.exists(dateFile)) {
			try {
				return Files.readAllBytes(dateFile)[0];
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		} else {
			return 0;
		}
	}
}
