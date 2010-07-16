package com.sixtydigits.cassandra;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;

import java.io.File;

/**
 * Starts an instance of the thrift cassandra daemon.
 *
 * @goal start
 * @phase pre-integration-test
 */
public class CassandraStartMojo
        extends AbstractMojo {
    private static final String CASSANDRA_HELPER = "CASSANDRA_HELPER";

    /**
     * Location of the cassandra.yaml file.
     *
     * @parameter expression="${maven.cassandra.cassandraConfig}"
     * @required
     */
    private File cassandraConfig;
    /**
     * Starts the cassandra daemon thread and then joins it.
     *
     * @parameter default-value="false" expression="${maven.cassandra.endless}"
     */
    private boolean endless;
    /**
     * Starts the cassandra daemon thread and then joins it.
     *
     * @parameter default-value="false" expression="${maven.cassandra.loadSchemaFromYaml}"
     */
    private boolean loadSchemaFromYaml;

    public void execute()
            throws MojoExecutionException {
        if (!super.getPluginContext().containsKey(CASSANDRA_HELPER)) {
            getLog().info("Starting Cassandra...");
            CassandraDaemonHelper helper = new CassandraDaemonHelper(getLog(), cassandraConfig.getAbsolutePath(), loadSchemaFromYaml);
            helper.start();
            super.getPluginContext().put(CASSANDRA_HELPER, helper);


            if (endless) {
                getLog().info("Entering endless mode...");
                try {
                    while (true) {
                        try {
                            Thread.sleep(1000l);
                        } catch (InterruptedException e) {
                        }
                    }
                } finally {
                    helper.stop();
                }
            }
        } else {
            getLog().info("Cassandra maven plugin is already in the plugin context...");
        }
    }

    public File getCassandraConfig() {
        return cassandraConfig;
    }

    public void setCassandraConfig(File cassandraConfig) {
        this.cassandraConfig = cassandraConfig;
    }

    public boolean isEndless() {
        return endless;
    }

    public void setEndless(boolean endless) {
        this.endless = endless;
    }

    public boolean isLoadSchemaFromYaml() {
        return loadSchemaFromYaml;
    }

    public void setLoadSchemaFromYaml(boolean loadSchemaFromYaml) {
        this.loadSchemaFromYaml = loadSchemaFromYaml;
    }
}
