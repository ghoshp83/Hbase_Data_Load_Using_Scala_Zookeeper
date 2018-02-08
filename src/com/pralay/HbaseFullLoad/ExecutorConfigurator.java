package com.pralay.HbaseFullLoad;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import java.io.IOException;

public class ExecutorConfigurator {
    private Configuration configuration;
    private FilterUtil filterUtil;

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
