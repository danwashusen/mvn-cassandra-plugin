package com.sixtydigits.cassandra;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.thrift.CassandraDaemon;
import org.apache.maven.plugin.logging.Log;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.ConnectException;
import java.net.URL;
import java.net.URLClassLoader;

import static java.lang.String.format;

public class CassandraDaemonHelper {
    private static final String STORAGE_CONFIG = "cassandra.yaml";

    private static CassandraDaemon cassandraDaemon;
    private Thread thread;

    private File config;
    private Log log;
    private boolean loadSchemaFromYaml;

    public CassandraDaemonHelper(Log log, String config, boolean loadSchemaFromYaml) {
        this.log = log;
        this.loadSchemaFromYaml = loadSchemaFromYaml;
        this.config = new File(config);

        if (!this.config.exists() || !this.config.isFile()) {
            throw new IllegalStateException(format("%s does not exist or it's not a file", config));
        }

        if (!STORAGE_CONFIG.equals(this.config.getName())) {
            throw new IllegalStateException(format("%s is a file but it's not the correctly named '%s'", config, STORAGE_CONFIG));
        }

        addConfigToClasspath();
    }

    private void addConfigToClasspath() {
        try {
            Method method = URLClassLoader.class.getDeclaredMethod("addURL", new Class[]{URL.class});
            method.setAccessible(true);
            method.invoke(DatabaseDescriptor.class.getClassLoader(), new Object[]{config.getParentFile().toURI().toURL()});
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public synchronized void start() {
        if (cassandraDaemon != null) {
            log.info("Cassandra maven plugin is already started...");
            return;
        }

        if (isServerRunning()) {
            log.info("Cassandra instance is already running...");
            return;
        }

        cleanDirs();

        cassandraDaemon = new CassandraDaemon();
        try {
            cassandraDaemon.init(null);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to init cassandra", e);
        }
        thread = new Thread(
                new Runnable() {
                    public void run() {
                        log.info("Start Cassandra...");
                        cassandraDaemon.start();
                    }
                }
        );
        thread.setDaemon(false);
        thread.start();

        if (loadSchemaFromYaml) {
            boolean keepTrying = true;
            while (keepTrying)
                try {
                    URL url = new URL("http://" + DatabaseDescriptor.getRpcAddress().getHostAddress() + ":8081/invoke?operation=loadSchemaFromYAML&objectname=org.apache.cassandra.service:type%3DStorageService");
                    log.info("Invoking " + url.toString() + " to load schema from YAML");
                    url.getContent();
                    keepTrying = false;
                } catch (ConnectException e) {
                    log.info("Could not connect, waiting for MX4J server to start");
                    sleepForABit(1000l);
                    continue;
                } catch (IOException e) {
                    keepTrying = false;
                }
        }

        Runtime.getRuntime().addShutdownHook(
                new Thread(
                        new Runnable() {
                            @Override
                            public void run() {
                                stop();
                            }
                        }
                )
        );
    }

    private void sleepForABit(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
        }
    }

    private boolean isServerRunning() {
        String host = DatabaseDescriptor.getRpcAddress().getHostAddress();
        int port = DatabaseDescriptor.getRpcPort();

        TTransport transport = new TSocket(host, port);

        try {
            transport.open();
            return true;
        } catch (TTransportException e) {
            return false;
        } finally {
            transport.close();
        }
    }

    private void cleanDirs() {
        log.info("Cleaning Cassandra data directories...");
        deleteDir(DatabaseDescriptor.getCommitLogLocation());

        for (String location : DatabaseDescriptor.getAllDataFileLocations()) {
            deleteDir(location);
        }
    }

    private void deleteDir(String name) {
        log.info("Deleting directory: " + name);
        File file = new File(name);
        if (file.exists()) {
            try {
                FileUtils.deleteRecursive(file);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to delete cassandra dir: " + name, e);
            }
        }
    }

    void stop() {
        // cassandra can't be stopped and started in the same VM
        log.info("Stopping Cassandra...");
        cassandraDaemon.stop();
        cassandraDaemon.destroy();
        try {
            thread.join();
        } catch (InterruptedException e) {
            log.error("Interrupted while stopping Cassandra...");
            // do nothing
        }
    }
}