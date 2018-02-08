package com.pralay.HbaseFullLoad;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.log4j.Logger;
import org.apache.hadoop.security.UserGroupInformation;

public class InitTasks {

    private static final Logger LOGGER = Logger.getLogger(InitTasks.class);

    public static void createHbaseTable(HBaseAdmin hbaseAdmin, String tableName, Integer cfTTL, int numberOfRegions,
                                        Compression.Algorithm compressionAlgorithm) throws IOException {

        if (hbaseAdmin.tableExists(tableName) == false) {
            HTableDescriptor hTableDescriptor = createHTableDescriptor(tableName, cfTTL, compressionAlgorithm);
            if (numberOfRegions > 1) {
                byte[][] splitKeysBytes = createSplitKeys(numberOfRegions);
                LOGGER.info("HBase table " + tableName + " will be created with presplit regions");
                hbaseAdmin.createTable(hTableDescriptor, splitKeysBytes);
            } else {
                LOGGER.info("HBase table " + tableName + " will be created with one region");
                hbaseAdmin.createTable(hTableDescriptor);
            }
        }

        return;
    }

    public static void createPhoenixView(String tableName, String jdbcConnection, String sqlQuery) throws SQLException  {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = DriverManager.getConnection(jdbcConnection);
            stmt = conn.createStatement();
            stmt.executeUpdate(sqlQuery);
        } finally {
            if (stmt != null) {
                stmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }

    public static HTableDescriptor createHTableDescriptor(String tableName, Integer cfTTL,
                                                          Compression.Algorithm compressionAlgorithm) {
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor cFamily = new HColumnDescriptor(toBytes("data"));

        cFamily.setCompressionType(compressionAlgorithm);
        cFamily.setTimeToLive(cfTTL);
        hTableDescriptor.addFamily(cFamily);

        return hTableDescriptor;
    }

    public static byte[][] createSplitKeys(int numberOfRegions) {
        long digits = Math.round(Math.ceil(Math.log10(numberOfRegions))) + 1;
        if (numberOfRegions / 10.0 == Math.round(numberOfRegions / 10.0)) digits--;
        // The first region will be set from '' to the first item in the array, so we skip the first number (eg. 00)
        // and start insertion with the second one (eg. 01)
        byte[][] splitKeysBytes = new byte[numberOfRegions - 1][];
        for (int i = 1; i < numberOfRegions; i++) {
            splitKeysBytes[i - 1] = String.format("%1$0" + digits + "d",
                    Math.round((float) i / numberOfRegions * Math.pow(10, digits))).getBytes();
        }
        return splitKeysBytes;
    }

    // Creates a HDFS folder path if it doesn't exist
    public static void createHdfsFolder(String hdfsFolderName,
                  com.pralay.HbaseFullLoad.Configuration cconf ) throws IOException {

        Path hdfsPath = new Path(hdfsFolderName);
        String hbase_hdfs_site_xml = cconf.getValue("HBASE_HDFS_SITE_XML",
                                       "/etc/hbase/conf/hbase-hdfs-site.xml");
        String hbase_hdfs_core_xml = cconf.getValue("HBASE_CORE_SITE_XML",
                                      "/etc/hbase/conf/hbase-core-site.xml");
        Configuration conf = new Configuration(false);
        conf.addResource( new Path(hbase_hdfs_site_xml));
        conf.addResource( new Path(hbase_hdfs_core_xml));
        FileSystem fs = FileSystem.get(conf);

        // Check whether hdfs folder exists
        if (fs.exists(hdfsPath) == false) {
            // Create hdfs folder if it doesn't exist
            fs.mkdirs(hdfsPath);
        }

        fs.close();
    }

    public static void start() {
       final com.pralay.HbaseFullLoad.Configuration configuration = com.pralay.HbaseFullLoad.Configuration.getInstance();

        String keyTab = configuration.getValue("KEY_TAB_FILE", "/etc/test.keytab");
        String testUser = configuration.getValue("TEST_USER", "test");
        try {
        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(testUser, keyTab);
        UserGroupInformation.setLoginUser(ugi);
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws IOException {
                    start1(configuration);
                    return null;
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void start1(com.pralay.HbaseFullLoad.Configuration  configuration) {

        final int DEFAULT_NUMBER_OF_REGIONS = 100;
        String outputTable = configuration.getValue("outputTable");

        Integer cfTTL = Integer.valueOf(configuration.getValue("cfTTL"));

        String zkReadyFolder = configuration.getValue("zkReadyFolder");
        String zkProcessedFolder = configuration.getValue("zkProcessedFolder");
        String zkMonitoringFolder = configuration.getValue("zkMonitoringFolder") + "/" + configuration.getValue("executionMode");

        String hdfsFolderName = configuration.getValue("hdfsTempFolder");

        HBaseAdmin hbaseAdmin = null;

        try {

            String compression = configuration.getValue("hbaseTableCompression", "GZ");
            Compression.Algorithm compressionAlgorithm;
            try {
                compressionAlgorithm = Compression.Algorithm.valueOf(compression.toUpperCase());
            } catch (IllegalArgumentException e) {
                LOGGER.error("Invalid compression algorithm (" + compression + ") specified, using GZ.");
                compressionAlgorithm = Compression.Algorithm.GZ;
            }
            Configuration conf = HBaseConfiguration.create();
            hbaseAdmin = new HBaseAdmin(conf);
            int numberOfRegions;
            try {
                numberOfRegions = Integer.valueOf(configuration.getValue("hbaseNumberOfRegions", "100"));
            } catch (NumberFormatException e) {
                LOGGER.error("Wrong number specified for number of regions for HBase table " + outputTable
                        + ", using default value: " + DEFAULT_NUMBER_OF_REGIONS);
                numberOfRegions = DEFAULT_NUMBER_OF_REGIONS;
            }
            createHbaseTable(hbaseAdmin, outputTable, cfTTL,
                    numberOfRegions, compressionAlgorithm);
            String sqlQuery = configuration.getValue("sqlCreateViewQuery",
                    "create view if not exists \""+outputTable+"\" (" +
                    "\"userid_ts\" varchar primary key, " +
                    "\"data\".\"payload\" varchar, " +
                    "\"data\".\"location_history\" varchar) " +
                    "immutable_rows=true");
            String jdbcConnection = configuration.getValue("phoenixJDBCConnString",
                    "jdbc:phoenix:node1.zookeeper.test," +
                    "node2.zookeeper.test," +
                    "node3.zookeeper.test:2181");
            createPhoenixView(outputTable, jdbcConnection, sqlQuery);

            ZookeeperManagerSingleton.getInstance().createZookeeperPath(zkReadyFolder);
            ZookeeperManagerSingleton.getInstance().createZookeeperPath(zkProcessedFolder);
            ZookeeperManagerSingleton.getInstance().createZookeeperPath(zkMonitoringFolder);

            createHdfsFolder(hdfsFolderName, configuration);

         } catch (IOException | SQLException e) {
            LOGGER.error("Problem occured while creating HBASE-related dependencies!", e);
            if (hbaseAdmin != null) {
                try {
                    hbaseAdmin.close();
                } catch (IOException e1) {
                    LOGGER.error(e1.toString());
                }
            }
            throw new RetryableException(e);
        }
    }
}
