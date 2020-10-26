package com.zhousf.hudi.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * sparkSession builder helper
  **/
object SparkSessionBuilder {
  def createSparkSession(appName: String, map: Map[String, String] = Map.empty): SparkSession = {

    val sparkConf = new SparkConf().set("spark.sql.broadcastTimeout", "600")
      .set("spark.debug.maxToStringFields", "500")
      //      .set("hive.metastore.uris", "thrift://10.1.1.1:10000")
      //      .set("spark.sql.warehouse.dir", "hdfs://user/hive/apache_warehouse")
      .set("spark.network.timeout", "300s")
      .set("spark.oozie.action.rootlogger.log.level", "DEBUG")
      .set("spark.sql.parquet.binaryAsString", "true")
      .set("hive.exec.dynamic.partition", "true")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      .set("spark.task.maxFailures", "5")
      .set("spark.sql.hive.convertMetastoreParquet", "false")
      //      .set("fs.defaultFS", "hdfs://")
      .setAll(
      Seq(
        ("spark.shuffle.sort.bypassMergeThreshold", "800"),
        ("spark.sql.shuffle.partitions", "100"),
        ("spark.sql.broadcastTimeout", "36000"),
        ("spark.sql.autoBroadcastJoinThreshold", "20971520"),
        ("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      )
    )

    if (map.nonEmpty) {
      for (key <- map.keySet) {
        sparkConf.set(key, map(key))
      }
    }

    SparkSession
      .builder()
      .appName(appName).master("local[*]")
      .config(sparkConf)
      //      .enableHiveSupport()
      .getOrCreate()
  }

}
