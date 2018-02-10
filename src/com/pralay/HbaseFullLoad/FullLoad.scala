package com.pralay.HbaseFullLoad

import java.io.IOException
import java.security.PrivilegedExceptionAction
import java.util.UUID
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.SaslRpcServer.AuthMethod

object FullLoad extends Serializable {

  val loggerName = this.getClass.getName
  @transient lazy val logger = Logger.getLogger(loggerName)

  def loadData (sc: SparkContext, filterutil: FilterUtil, params: Array[String] ): Unit = {
    val configuration= com.pralay.HbaseFullLoad.Configuration.getInstance()
    val keyTab = configuration.getValue("KEY_TAB_FILE", "/etc/test.keytab")
    val testUser = configuration.getValue("TEST_USER", "test")
    val hbase_hdfs_site_xml = configuration.getValue("HBASE_HDFS_SITE_XML", "/etc/hbase/conf/hbase-hdfs-site.xml")
    val hbase_hdfs_core_xml = configuration.getValue("HBASE_CORE_SITE_XML", "/etc/hbase/conf/hbase-core-site.xml")
    try {
      // Configure HBase connection
      val conf1 = new org.apache.hadoop.conf.Configuration(false)
      conf1.addResource(new Path(hbase_hdfs_core_xml))
      conf1.addResource(new Path(hbase_hdfs_site_xml))

      val hbaseConf = HBaseConfiguration.create(conf1)
      logger.info("alter the hbaseConf:" + hbaseConf)
      UserGroupInformation.setConfiguration(hbaseConf);
      val ugi: UserGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI(testUser, keyTab)
      ugi.setAuthenticationMethod(AuthMethod.KERBEROS);
      UserGroupInformation.setLoginUser(ugi)
      UserGroupInformation.setConfiguration(hbaseConf)
      ugi.doAs(new PrivilegedExceptionAction[Void]() {
        @throws[IOException]
        def run: Void = {
          loadData1(sc, filterutil, params, hbaseConf)
          null
        }
      })
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  def loadData1(sc: SparkContext, filterutil: FilterUtil, params: Array[String],
                hbaseConf: org.apache.hadoop.conf.Configuration) {


    if (params.length != 4) {
      println("Arguments: <dataSource> <numPartitions> <HBaseTable> <HDFSTempFolder>")
      sys.exit()
    }

    val paths = params(0) // Folder of input files on HDFS
    val numPartitions = params(1).toInt // Number of final partitions
    val hbaseTable = params(2) // Name of Hbase table to load data
    val hdfsTempFolder = params(3)
    var startTime = System.currentTimeMillis()
    val start = startTime
    logger.info("DataType1 BulkLoad process started for " + paths + " folders.")

    // Configure HBase connection
    val tableName = hbaseTable
    val HTable = new HTable(hbaseConf, tableName)
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val job = Job.getInstance(hbaseConf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoad(job, HTable)

    val hbaseAdmin = new HBaseAdmin(hbaseConf)
    val hbaseRegions = hbaseAdmin.getTableRegions(tableName.getBytes)
    val hbasePartitioner = new HBasePartitioner(hbaseRegions)

    val fs = FileSystem.get(hbaseConf)

    if (!fs.exists(new Path(hdfsTempFolder))) {
      fs.mkdirs(new Path(hdfsTempFolder))
      logger.info("Hdfs temp folder recreated " + hdfsTempFolder)
    }

    var finishTime = System.currentTimeMillis()
    logger.info("HBase connection configured, took " + (finishTime - startTime).toString + " ms.")
    startTime = finishTime
    val file = paths.split(",").map(path => sc.textFile(path)).reduce(_ union _)

    finishTime = System.currentTimeMillis()
    logger.info("HDFS content read, took " + (finishTime - startTime).toString + " ms.")
    startTime = finishTime
    
      // Mapping
      val mappedContent = file
        .map(line => line.split('\t'))
        .filter(members => filterutil.confirmEsrSplit(members.length))
        .map(members => (members(1).reverse + "-" + members(0), members(3)))
        .repartitionAndSortWithinPartitions(hbasePartitioner)

      val esrMap = mappedContent
        .map(KVTuple => {
          try {
              val esrKV: KeyValue = new KeyValue(KVTuple._1.getBytes, "data".getBytes(), "payload".getBytes(), KVTuple._2.getBytes())
              (new ImmutableBytesWritable(KVTuple._1.getBytes), esrKV)
          } catch {
              case e : Exception =>
              FullLoad.logger.error(e.getStackTrace().toString(),e)
              (None, None)
          }
        }).filter(members => members._1 != None)


      finishTime = System.currentTimeMillis()
      logger.info("Mapping completed, took " + (finishTime - startTime).toString + " ms.")
      startTime = finishTime

      // Set path for HFiles output
      val esrMapHfilePath = hdfsTempFolder + UUID.randomUUID

      logger.info("DataType1 HFile output path on hdfs: " + esrMapHfilePath)
      try {
        // Save Hfiles on HDFS
        esrMap.saveAsNewAPIHadoopFile(
          esrMapHfilePath,
          classOf[ImmutableBytesWritable],
          classOf[KeyValue],
          classOf[HFileOutputFormat2],
          hbaseConf)

        finishTime = System.currentTimeMillis()
        logger.info("HFiles saved to HDFS, took " + (finishTime - startTime).toString() + " ms.")
        startTime = finishTime

        // Bulk load Hfiles to HBase
        val fullLoad = new LoadIncrementalHFiles(hbaseConf)
        fullLoad.doBulkLoad(new Path(esrMapHfilePath), HTable)

        finishTime = System.currentTimeMillis()
        logger.info("HFile bulkLoad completed, took " + (finishTime - startTime).toString + " ms.")
        startTime = finishTime
      } catch {
        case e: Exception =>
          logger.warn("Exception during bucket load " + paths, e)
          throw e
      }
      finally {
        startTime = System.currentTimeMillis()
        // Delete temporary HDFS content
        val fs = FileSystem.get(hbaseConf)
        fs.delete(new Path(hdfsTempFolder), true)
        logger.info("Hdfs temp folder deleted " + hdfsTempFolder)
      }
    

    finishTime = System.currentTimeMillis()
    logger.info("Temporary HFile deleted from HDFS, took " + (finishTime - startTime).toString() + " ms.")

    logger.info("Process completed, took " + (finishTime - start).toString() + " ms in overall. Exiting.")

  }

}
