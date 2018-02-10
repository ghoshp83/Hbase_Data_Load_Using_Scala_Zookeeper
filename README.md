# Hbase_Data_Load_Using_Scala_Zookeeper

1. This application is used to load the data present in HDFS to HBase tables using scala and zookeeper. 
2. The data is getting stored in HDFS by upstream application in time bucket manner. 
3. In every five minutes, new data are getting loaded in HDFS and a reference is getting stored in zookeeper. 
4. This application is following zookeeper's data nodes. Zookeeper's data nodes contain information about which time-bucket of data is
   ready to be processed.
5. 
