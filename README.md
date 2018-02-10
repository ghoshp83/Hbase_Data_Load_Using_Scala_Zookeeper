# Hbase_Data_Load_Using_Scala_Zookeeper

1. This application is used to load the data present in HDFS to HBase tables using scala and zookeeper. 
2. The data is getting stored in HDFS by upstream application in time bucket manner. 
3. In every five minutes, new data are getting loaded in HDFS and a reference is getting stored in zookeeper. 
4. This application is following zookeeper's znodes. Zookeeper's znodes contain information about which time-bucket of data is ready to be
   processed.
5. Data is stored in a epoch timestamp bucket. For example ->
   ```
   hadoop fs -ls /test/datatype1
   /test/datatype1/1517805000
   /test/datatype1/1517805300
   /test/datatype1/1517805600
   ```
6. Zookeeper is also storing the epoch timestamp bucket in a specified znode. This application will monitor znodes and process the
   appropriate time bucket data and load them into Hbase tables.
7. This application contains two configuration files ->
   a) config.xml 
   b) log4j.xml
8. config.xml contains application configuration details, Hbase configuration details, HDFS configuration details, zookeeper configuration
   details and optional other database(greenplum and oracle) details.
9. log4j.xml contains application log mechanism details.
10. All dependent jar files are present in this repository in "dependencies" folder.
11. One can build the application from the source code 
