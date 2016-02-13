package com.rei.trailregister;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.Driver;

import org.junit.Test;

public class DriverDownloaderTest {

    private static final String MYSQL_DOWNLOAD_URL = 
            "http://search.maven.org/remotecontent?filepath=mysql/mysql-connector-java/5.1.38/mysql-connector-java-5.1.38.jar";
    
    private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";

    @Test
    public void canDownloadAndInstantiateJar() throws IOException {
        DriverDownloader downloader = new DriverDownloader(MYSQL_DOWNLOAD_URL, MYSQL_DRIVER_CLASS);
        try {
            Driver driver = downloader.get();
            assertEquals(MYSQL_DRIVER_CLASS, driver.getClass().getName());
        } finally {
            downloader.getDriverJar().toFile().deleteOnExit();
        }
    } 

}
