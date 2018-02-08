package com.pralay.HbaseFullLoad;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.token.TokenUtil;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Executor implements ZookeeperManagerSingleton.DataMonitorListener {

    public static int ZOOKEPER_RETRY = 2;
    public static int EXECUTOR_RETRY = 10;

    private static final Logger LOGGER = Logger.getLogger(Executor.class);
    private static Executor instance;

    private String executionMode;

    private String resolveLocation;

    private static SparkConf sparkConf = null;
    private static SparkContext sc = null;
    private static UserGroupInformation ugi = null;

    private static FilterUtil filter;

    private String partitions;
    private String hbaseOutputTable;
    private String hbaseIcidResolveTable;

    private String znodeReadyFolder;
    private String znodeProcessedFolder;

    private String hdfsTempFolder;

    private Integer locDataUpdateFreq;

    private long lastLocUpdateTime;

    private List<Integer> ProcessTimeArray = new ArrayList<Integer>();

    private int avgOfLastN = 12;
    private int numberOfBucketsToProcessInOneIteration = 1;

    Configuration config;
    private Throwable exceptionInExecutor;
    private boolean closing;

    public Executor(ExecutorConfigurator executorConfigurator) {
        Configuration configuration = executorConfigurator.getConfiguration();
        FilterUtil filter = executorConfigurator.getFilterUtil();

        // Init Spark context - only one context can exist, so initialize only once
        if (sparkConf == null && sc == null) {
            try {
                sparkConf = executorConfigurator.getSparkConf("HBaseFullLoad");
                LOGGER.info("before creating spark context, spark Conf:" + sparkConf);
                // UserGroupInformation.setConfiguration(SparkHadoopUtil.get().newConfiguration(sparkConf));
                if (UserGroupInformation.isSecurityEnabled()) {
                    Credentials credentials = UserGroupInformation.getLoginUser().getCredentials();
                    SparkHadoopUtil.get().addCurrentUserCredentials(credentials);
                }
                sc = executorConfigurator.getSparkContext(sparkConf);
            } catch (Throwable th) {
                throw new FatalException(th);
            }
        }

        this.config = configuration;

        this.executionMode = config.getValue("executionMode");
        this.numberOfBucketsToProcessInOneIteration = Integer.parseInt(config.getValue("numberOfBucketsToProcessInOneIteration", "1"));

        //this.resolveLocation = config.getValue("resolveLocation");

        this.partitions = config.getValue("partitions");
        this.hbaseOutputTable = config.getValue("outputTable");
        this.hbaseIcidResolveTable = config.getValue("icidResolveTable");

        this.znodeReadyFolder = config.getValue("zkReadyFolder");
        this.znodeProcessedFolder = config.getValue("zkProcessedFolder");

        this.hdfsTempFolder = config.getValue("hdfsTempFolder");

        if (this.executionMode.equals("datatype1")) {
            // Frequency of location update in hours
            this.locDataUpdateFreq = Integer.parseInt(config.getValue("locDataUpdateFreq"));
            //this.lr = executorConfigurator.getLocationResolver();

        }

        this.filter = filter;

        // Record execution starting time
        this.lastLocUpdateTime = System.currentTimeMillis() / 1000L;
        LOGGER.info("MONITOR: last update of cell location data: " + lastLocUpdateTime);

        // Zabbix event monitoring
        // LOGGER.info("MONITOR: ApplicationID: " + String.valueOf(sc.applicationId()));
        ZookeeperManagerSingleton.getInstance().writeMonitoringParam("ApplicationID", String.valueOf(sc.applicationId()));
    }

    public static void main(String args[]) {

        if (args.length != 1) {
            LOGGER.error("Invalid number of arguments. (" + args.length + ")");
            throw new IllegalArgumentException("Arguments: <configPath>");
        }

        startInstance(args[0]);
        throw new RuntimeException("Something unexpected happen check logs for further information");
    }

    public static void startInstance(String config) {
        int repeatCounter = EXECUTOR_RETRY;
        boolean repeat = true;
        while (repeat && repeatCounter>0) {
            try {
                // Will get configuration values from XML from hdfs
                Configuration.init(config);
                // Get configuration values from XML from hdfs
                Configuration configuration = Configuration.getInstance();

                // Initialize Zookeeper
                ZookeeperManagerSingleton.getInstance().init();

                // Creates resources if don't exist (Hbase Table, Zookeeper paths, HDFS temp folder)
                InitTasks.start();

                // Filtering
                filter = new FilterUtil();

                // create & run Executor instance
                instance = new Executor(ExecutorConfigurator.createExecutorConfigurator(configuration, filter));
                instance.run();
            } catch (FatalException | InterruptedException e) {
                repeat = exitBecauseOfError(e);
            } catch (Exception re) {
                repeat = waitOrExitOnError(re);
            } catch (Throwable th) {
                repeat = exitBecauseOfError(th);
            }
            repeatCounter--;
        }
    }

    private static boolean waitOrExitOnError(Exception re) {
        LOGGER.error("Following problem found during execution: ", re);
        LOGGER.error("Retrying after 1 minute ...");
        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
            instance = null;
            LOGGER.error("Following problem found during waiting for next retry: " + re.getMessage() + "\n" +
                    "Exiting...");
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private static boolean exitBecauseOfError(Throwable e) {
        LOGGER.error("Following problem found during execution: " + e.getMessage() + "(Error class: " + e.getClass() + ")\n" +
                "Exiting...");
        e.printStackTrace();
        return false;
    }

    public void run() throws Throwable {
        closing = false;
        ZookeeperManagerSingleton.getInstance().setAddedChildrenListener(znodeReadyFolder, this, znodeProcessedFolder);

        synchronized (this) {
            while (closing == false) {
                wait();
            }
        }

        if (exceptionInExecutor != null) {
            throw exceptionInExecutor;
        }
    }

    @Override
    public void closing(Throwable exc) {
        synchronized (this) {
            closing = true;
            this.exceptionInExecutor = exc;
            notifyAll();
        }
    }

    // implement ZookeeperManagerSingleton.DataMonitorListener
    // Called in case of new folder path placed into the znode
    @Override
    public void trigger(List<String> newFolders) {
        LOGGER.info("Get notification.");
        // At startWatcher, more than one new values can arrive in the parameter. They
        // are handled one by one

        // Check whether cell location data update is necessary in case of datatype1 and enable location resolution.
        if (executionMode.equals("datatype1") && resolveLocation.equals("true")) {
            // Check whether cell location data update is necessary.
            long currentTime = System.currentTimeMillis() / 1000L;

            // In case of cell location data is outdated, refresh it
            if ((currentTime - lastLocUpdateTime) > (locDataUpdateFreq * 60 * 60)) {
                // Update cell location data
                //lr.setConfiguration(config);
                LOGGER.info("Cell location data updated successfully.");

                // Update last refresh date
                lastLocUpdateTime = currentTime;
                writeMonitoringParam("CellLocLastUpdate", String.valueOf(lastLocUpdateTime));
            }
        }

        for (Iterator<String> iter = newFolders.iterator(); iter.hasNext(); ) {
            long bucketProcessStart = System.currentTimeMillis() / 1000L;

            String folder = "";
            String timeStamps = "";
            Map<String, String> processedZnodePaths = new HashMap();

            int numberOfBuckets = 0;
            for (; iter.hasNext() && numberOfBuckets < numberOfBucketsToProcessInOneIteration; numberOfBuckets++) {
                String item = iter.next();
                String timeStamp = item.substring(item.lastIndexOf("=") + 1, item.length());
                String processedZnodePath = znodeProcessedFolder + "/" + timeStamp;

                if (ZookeeperManagerSingleton.getInstance().exists(processedZnodePath) == null) {
                    monitoringBefore(newFolders, item, timeStamp, bucketProcessStart);
                    timeStamps += "," + timeStamp;
                    if (folder.isEmpty()) {
                        folder += item;
                    } else {
                        folder += "," + item;
                    }
                    processedZnodePaths.put(processedZnodePath, item);
                } else {
                    LOGGER.info("Received DataType1 has already been processed! " + item);
                    --numberOfBuckets;
                }
            }

            if (!folder.isEmpty()) {
                LOGGER.info("newValue: " + folder);

                boolean bucketSuccess = true;
                try {
                    if (executionMode.equals("datatype1")) {
                    	FullLoad.loadData(sc, filter, new String[] { folder, partitions, hbaseOutputTable, hdfsTempFolder });
                    } 
                } catch( Exception e) {
                    LOGGER.error("Unsuccessful bucket load: " + folder, e);
                    bucketSuccess = false;
                }

                if(bucketSuccess) {
                    for (Map.Entry<String, String> entry : processedZnodePaths.entrySet()) {
                        ZookeeperManagerSingleton.getInstance().create(entry.getKey(), entry.getValue().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        LOGGER.info("Processed znode created at " + entry.getKey());
                    }
                }

                filter.resetCounters();

                monitoringAfter(folder, timeStamps, bucketProcessStart, bucketSuccess, numberOfBuckets);

                // Explicitly removes the content of the appcache
                com.pralay.HbaseFullLoad.Environment.collectGarbage(sc.applicationId());

            }
        }
        LOGGER.info("Waiting for new znode...");
    }

    private void monitoringBefore(List<String> newFolders, String folder, String timeStamp, long bucketProcessStart) {
        if (!ProcessTimeArray.isEmpty()) {
            Integer processTimeSum = 0;
            for (Integer actualProcessTime : ProcessTimeArray) {
                processTimeSum += actualProcessTime;
            }
            int avgProcessTime = processTimeSum / ProcessTimeArray.size();

            ZookeeperManagerSingleton.getInstance().writeMonitoringParam("AvgBucketProcessTime", String.valueOf(avgProcessTime));
        }

        ZookeeperManagerSingleton.getInstance().writeMonitoringParam("CurrentProcessPath", folder);
        ZookeeperManagerSingleton.getInstance().writeMonitoringParam("CurrentProcessTS", String.valueOf(timeStamp));

        long overallDelay = bucketProcessStart - Long.valueOf(timeStamp);
        ZookeeperManagerSingleton.getInstance().writeMonitoringParam("DelayOverallHBaseLoad", String.valueOf(overallDelay));

        long bulkloaderDelay = bucketProcessStart - Long.valueOf(timeStamp);
        ZookeeperManagerSingleton.getInstance().writeMonitoringParam("DelayBulkLoader", String.valueOf(bulkloaderDelay));

        int bucketsNotProcessed = 0;

        for (String path : newFolders) {
            String currentTS = path.substring(path.lastIndexOf("=") + 1, path.length());
            if (Long.valueOf(currentTS).compareTo(Long.valueOf(timeStamp)) > 0) {
                bucketsNotProcessed++;
            }
        }
        ZookeeperManagerSingleton.getInstance().writeMonitoringParam("NumberOfUnprocessedBucketsAhead", String.valueOf(bucketsNotProcessed));
    }

    private void monitoringAfter(String folder, String timeStamps, long bucketProcessStart, boolean bucketSuccess, int numberOfBuckets) {
        Stat stat = new Stat();
        ZookeeperManagerSingleton.getInstance().getData(znodeReadyFolder, false, stat);
        ZookeeperManagerSingleton.getInstance().writeMonitoringParam("ZnodesInReady", String.valueOf(stat.getNumChildren()));

        ZookeeperManagerSingleton.getInstance().getData(znodeProcessedFolder, false, stat);
        ZookeeperManagerSingleton.getInstance().writeMonitoringParam("ZnodesInProcessed", String.valueOf(stat.getNumChildren()));

        if( bucketSuccess ) {
            LOGGER.info("EDCR BulkLoad filtered split drop: " + filter.getSplitDrop().toString());

            ZookeeperManagerSingleton.getInstance().writeMonitoringParam("lastProcessedPath", folder);

            ZookeeperManagerSingleton.getInstance().writeMonitoringParam("lastProcessedTS", timeStamps);

            long bucketProcessFinish = System.currentTimeMillis() / 1000L;

            if (numberOfBuckets == 0) {
                numberOfBuckets = 1; // make sure to not divide with 0
            }
            int processTime = (int) (bucketProcessFinish - bucketProcessStart) / numberOfBuckets;

            ZookeeperManagerSingleton.getInstance().writeMonitoringParam("lastProcessTime", String.valueOf(processTime));

            if (ProcessTimeArray.size() < avgOfLastN) {
                ProcessTimeArray.add(processTime);
            } else {
                // Shift right
                for (int index = ProcessTimeArray.size() - 2; index >= 0; index--) {
                    ProcessTimeArray.set(index + 1, ProcessTimeArray.get(index));
                }
                // Replace first element
                ProcessTimeArray.set(0, processTime);
            }
        }
    }

    void writeMonitoringParam(String nodeName, String param) {
        try {
            ZookeeperManagerSingleton.getInstance().writeMonitoringParam(nodeName, param);
        } catch (Exception e) {
            LOGGER.error("MONITORING: unsuccessful because of exception '" + e.getClass().getName() + "' with message: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static Executor getInstance() {
        return instance;
    }
}
