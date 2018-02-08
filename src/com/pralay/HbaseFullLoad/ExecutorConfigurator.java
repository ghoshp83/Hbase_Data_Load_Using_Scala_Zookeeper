package com.pralay.HbaseFullLoad;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class ExecutorConfigurator {
    private Configuration configuration;
    private FilterUtil filterUtil;
    //private LocationResolver locationResolver;

    public ExecutorConfigurator() {
    }

    public ExecutorConfigurator(Configuration configuration, FilterUtil filter) {
        this.configuration = configuration;
        this.filterUtil = filter;
    }

    public static ExecutorConfigurator createExecutorConfigurator(Configuration configuration, FilterUtil filter) {
        return new ExecutorConfigurator(configuration, filter);
    }

    public FilterUtil getFilterUtil() {
        return filterUtil;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    /*public LocationResolver getLocationResolver() {
        if (this.locationResolver == null && configuration.getValue("executionMode").equals("esr")) {
            // Fetches the actual content of the Cell location table
            // Location resolution is necessary only in case of ESR data source
            this.locationResolver = new LocationResolver();
            if (configuration.getValue("resolveLocation").equals("true")) {
                locationResolver.setConfiguration(configuration);
            }
        }
        return locationResolver;
    }*/

    public SparkConf getSparkConf(String hBaseBulkLoader) {
        return new SparkConf().setAppName("HBaseFullLoad");
    }

    public SparkContext getSparkContext(SparkConf sparkConf) {
        return new SparkContext(sparkConf);
    }

    public ZooKeeper getZookeeper(Watcher watcher) throws IOException {
        return new ZooKeeper(configuration.getValue("zkHost") + ":" + configuration.getValue("zkPort"), 3000, watcher);
    }
}
