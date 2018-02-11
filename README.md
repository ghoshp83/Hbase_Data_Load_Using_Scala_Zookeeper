# Hbase_Data_Load_Using_Scala_Zookeeper

1. This application is used to load the data present in HDFS to HBase tables using scala and zookeeper. 
2. The data is getting stored in HDFS by upstream application in time bucket manner. 
3. In every thirty minutes, new data are getting loaded in HDFS and a reference is getting stored in zookeeper. 
4. This application is following zookeeper's znodes. Zookeeper's znodes contain information about which time-bucket of data is ready to be
   processed.
5. Data is stored in a epoch timestamp bucket. For example ->
   ```
   hadoop fs -ls /test/datatype1
   /test/datatype1/1517805000
   /test/datatype1/1517806800
   /test/datatype1/1517808600
   ```
6. Zookeeper is also storing the epoch timestamp bucket in a specified znode. This application will monitor znodes and process the
   appropriate time bucket data and load them into Hbase tables.
7. This application contains two configuration files ->
   a) config.xml 
   b) log4j.xml
8. config.xml contains application configuration details, Hbase configuration details, HDFS configuration details, zookeeper configuration
   details and optional other database(greenplum and oracle) details.
9. log4j.xml contains application log mechanism details.
10. All dependent jar files are present in this repository inside "dependencies" folder.
11. One can create executable(runnable jar file) from application source code using maven or gradle with source code present in this
    repository inside "src" folder along with "dependencies".
12. Main class of this application is "com.pralay.HbaseFullLoad.Executor"
13. All the zookeeper operations will be handled by "com.pralay.HbaseFullLoad.ZookeeperManagerSingleton". This class will be responsible
    for below activities ->
    
    a) create a singleton zookeeper instance.
    
    b) Initialize zookeeper session.
    
    c) Monitoring of znodes. When a new znode is getting created then a trigger will be initiated to process the data and load in Hbase.
    
    d) Exeception handling during connection loss with zookeeper
    
    e) There are there znodes where data management will be done :
    
       1. /test/datatype1/ready : Zookeeper path of znodes for data bulks created by upstream systems.
       
       2. /test/datatype1/processor : Zookeeper path of znodes for data BulkLoaded into HBase.
       
       3. /test/Monitoring/HBaseFullLoad : Zookeeper path of znodes for data BulkLoaded into HBase.
       
    f) Create, Modify and Delete znodes according to data processing flow. At first, data will be present in "ready" znode and then it 
       will move to "processor" and eventually to "HBaseFullLoad" znode.
       
14. Hbase management will be taken care by "com.pralay.HbaseFullLoad.InitTasks". It will ->
    
    1. Create Hbase connection through apache phoenix.  
    
    2. Create Hdfs location for Hbase table(s). 
    
    3. Setup the compression algorithm, TTL and required number of region server. It will be sourced from config.xml file.
    
    4. Create Hbase table, if didn't exist. 
    
    5. Perform all database transaction through phoenix.
    
15. Hbase tables has been partitioned through "com.pralay.HbaseFullLoad.HBasePartitioner" class to get better performance.

16. Data will be loaded into Hbase through a scala class "com.pralay.HbaseFullLoad.FullLoad".

17. Kerberos authentication has been enabled into this application. All hbase connection will be authenticated through "test.keytab"
    file. "test" will be the authenticated user to carry out business processing in secure manner. 
