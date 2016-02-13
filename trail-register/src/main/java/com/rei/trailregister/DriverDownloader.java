package com.rei.trailregister;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Driver;
import java.util.function.Supplier;

import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;

public class DriverDownloader implements Supplier<Driver> {
    private OkHttpClient client = new OkHttpClient();
    
    private String driverUrl;
    private String driverClass;

    private Driver driver;

    private Path driverJar;

    public DriverDownloader(String driverUrl, String driverClass) {
        this.driverUrl = driverUrl;
        this.driverClass = driverClass;
    }
    
    public Path getDriverJar() {
        return driverJar;
    }

    @Override
    public Driver get() {
        if (driver == null) {
            try {
                Request request = new Request.Builder().url(driverUrl).get().build();
                driverJar = Files.createTempFile("driver", ".jar");
                Files.delete(driverJar);
                Files.copy(client.newCall(request).execute().body().byteStream(), driverJar);
                
                @SuppressWarnings("resource") // we want this to stay open for the lifetime of the app
                URLClassLoader loader = new URLClassLoader(new URL[] { driverJar.toUri().toURL() });
                driver = (Driver) loader.loadClass(driverClass).newInstance();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                throw new RuntimeException("unable to create and load driver class!", e);
            }
        }
        return driver;
    }

}
