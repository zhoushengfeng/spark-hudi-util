package com.zhousf.hudi.util

import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.model.{EmptyHoodieRecordPayload, HoodieCleaningPolicy, HoodieTableType, OverwriteWithLatestAvroPayload}
import org.apache.hudi.config.HoodieIndexConfig
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.keygen.{ComplexKeyGenerator, SimpleKeyGenerator}
import org.apache.spark.sql._

/**
  * hudi utils
  *
  **/
object HudiUtils {

  implicit class HiveSupport[T](hudiConfig: DataFrameWriter[T]) {

    /**
      * @param hiveDataBaseName                hive database name
      * @param hiveTableName                   hive table name
      * @param hivePartitionFields             hive partition key
      * @param hivePartitionExtractorClassName The class to extract the partition fields
      * @param hiveUserName                    hive user name
      * @param hivePassword                    hive user password
      * @param hiveServer2URL                  hiveserver2 uri,like jdbc:hive2://host:10000
      * @return
      */
    def syncToHive(
                    hiveDataBaseName: String,
                    hiveTableName: String,
                    hivePartitionFields: String,
                    hivePartitionExtractorClassName: String = classOf[MultiPartKeysValueExtractor].getCanonicalName,
                    // change to your Cluster information
                    hiveUserName: String = "root",
                    hivePassword: String = "root",
                    hiveServer2URL: String = "jdbc:hive2://localhost:10000"
                  ): DataFrameWriter[T] = {

      hudiConfig.option(HIVE_SYNC_ENABLED_OPT_KEY, "true")
        .option(HIVE_DATABASE_OPT_KEY, hiveDataBaseName)
        .option(HIVE_TABLE_OPT_KEY, hiveTableName)
        .option(HIVE_USER_OPT_KEY, hiveUserName)
        .option(HIVE_PASS_OPT_KEY, hivePassword)
        .option(HIVE_URL_OPT_KEY, hiveServer2URL)
        .option(HIVE_PARTITION_FIELDS_OPT_KEY, hivePartitionFields)
        .option(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, hivePartitionExtractorClassName)
        .setHudiCompact()
    }


    /**
      * hudi入口
      *
      * @param upsertParallel upsert parallel,default  1500
      * @param insertParallel insert parallel,default 1500
      * @param deleteParallel delete parallel,default 1500
      * @return
      */
    def setHudiParallel(upsertParallel: Int = 1500, insertParallel: Int = 1500, deleteParallel: Int = 1500): DataFrameWriter[T] = {
      hudiConfig
        .option("hoodie.upsert.shuffle.parallelism", upsertParallel)
        .option("hoodie.insert.shuffle.parallelism", insertParallel)
        .option("hoodie.delete.shuffle.parallelism", deleteParallel)
    }

    /**
      *
      * @param cleanPolicy         the clean policy, 'KEEP_LATEST_COMMIT' or 'KEEP_LATEST_FILE_VERSIONS'
      * @param asyncClean          enable asynchronous cleanup
      * @param inlineCompact       enable inline Compact
      * @param minCommits          run a compaction every min delta commits
      * @param maxCommits          run a compaction every max delta commits
      * @param retainedCommit      keep the latest commit number
      * @param retainedFileVersion keep the latest file version number
      * @return
      */
    def setHudiCompact(
                        cleanPolicy: String = HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS.name,
                        asyncClean: Boolean = true,
                        inlineCompact: Boolean = false,
                        minCommits: Int = 20,
                        maxCommits: Int = 30,
                        retainedCommit: Int = 10,
                        retainedFileVersion: Int = 3
                      ): DataFrameWriter[T] = {
      hudiConfig
        .option("hoodie.cleaner.commits.retained", retainedCommit)
        .option("hoodie.keep.min.commits", minCommits)
        .option("hoodie.keep.max.commits", maxCommits)
        .option("hoodie.cleaner.policy", cleanPolicy)
        .option("hoodie.cleaner.fileversions.retained", retainedFileVersion)
        .option("hoodie.cleaner.parallelism", "200")
        .option("hoodie.clean.automatic", "true")
        .option("hoodie.clean.async", asyncClean)
        .option("hoodie.compact.inline", inlineCompact)
        .option("hoodie.compact.inline.max.delta.commits", "5")
        .option("hoodie.commits.archival.batch", minCommits / 2)
    }

    /**
      * @param tableName             hudi table namt
      * @param preCombineFields      merge comparison field, example:modify_time
      * @param recordKeyFields       recordKey show be unique,example: "timestamp,id" or "timestamp"
      * @param partitionPathFields   partitionKey ，example: "ds,platform_type" or "ds"
      * @param tableType             hudi table type，example: 'merge_on_read' or 'copy_on_write'
      * @param keyGeneratorClassName the class to extract the key fields  default = ComplexKeyGenerator
      * @param hiveStylePartitioning enable hive style partitons file name
      * @return
      */
    def setHudiTableConfig(
                            tableName: String,
                            preCombineFields: String,
                            recordKeyFields: String,
                            partitionPathFields: String = DEFAULT_PARTITIONPATH_FIELD_OPT_VAL,
                            tableType: String = HoodieTableType.MERGE_ON_READ.name,
                            keyGeneratorClassName: String = classOf[ComplexKeyGenerator].getName,
                            hiveStylePartitioning: Boolean = true
                          ): DataFrameWriter[T] = {
      hudiConfig
        .option(TABLE_NAME, tableName)
        .option(PRECOMBINE_FIELD_OPT_KEY, preCombineFields)
        .option(RECORDKEY_FIELD_OPT_KEY, recordKeyFields)
        .option(PARTITIONPATH_FIELD_OPT_KEY, partitionPathFields)
        .option(KEYGENERATOR_CLASS_OPT_KEY,
          if (recordKeyFields.contains(",") || partitionPathFields.contains(","))
            keyGeneratorClassName
          else
            classOf[SimpleKeyGenerator].getName)

        .option(HIVE_STYLE_PARTITIONING_OPT_KEY, hiveStylePartitioning)
        .option(TABLE_TYPE_OPT_KEY, tableType)
        .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
        .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name()) // index type GLOBAL_BLOOM

    }

    def extendedHudiConfig(extendedConfig: Seq[(String, String)]): DataFrameWriter[T] = {
      for (x <- extendedConfig) {
        hudiConfig.option(x._1, x._2)
      }
      hudiConfig
    }


  }


  implicit class WriteToHudi[T](writer: DataFrameWriter[T]) {

    private def hudi: DataFrameWriter[T] = {
      writer.format("org.apache.hudi").mode(SaveMode.Append)
    }


    /**
      * @param operateType   delete insert upsert(default value) buld_insert
      * @param isHardDeletes Soft Deletes or Hard Deletes , default Hard Deletes
      * @return
      */
    def saveToHudi(operateType: String = "upsert", isHardDeletes: Boolean = true): DataFrameWriter[T] = {
      operateType.toLowerCase.trim match {
        case "insert" => insertToHudi
        case "upsert" => upsertToHudi
        case "bulk_insert" => bulkInsertToHudi
        case "delete" => deleteFromHudi(isHardDeletes)
        case _ => throw new IllegalArgumentException(s"IllegalArgumentException: operateType= $operateType ")
      }
    }

    def upsertToHudi: DataFrameWriter[T] = {
      hudi.option(OPERATION_OPT_KEY, UPSERT_OPERATION_OPT_VAL)
    }

    def insertToHudi: DataFrameWriter[T] = {
      hudi.option(OPERATION_OPT_KEY, INSERT_OPERATION_OPT_VAL)
    }

    def bulkInsertToHudi: DataFrameWriter[T] = {
      hudi.option(OPERATION_OPT_KEY, BULK_INSERT_OPERATION_OPT_VAL)
    }

    def deleteFromHudi(isHardDeletes: Boolean = true): DataFrameWriter[T] = {
      hudi.option(OPERATION_OPT_KEY, DELETE_OPERATION_OPT_VAL)
        .option(PAYLOAD_CLASS_OPT_KEY,
          if (isHardDeletes)
            classOf[EmptyHoodieRecordPayload].getName
          else
            classOf[OverwriteWithLatestAvroPayload].getName)
    }
  }


  implicit class ReadFromHudi[T](reader: DataFrameReader) {

    private def buildPartitionPath(path: String, partition: String): String = {
      if (path == null || path.trim == "") {
        throw new IllegalArgumentException(s"ERROR :IllegalArgument path = '$path' ")
      }
      val stringBuilder = new StringBuilder()
      stringBuilder.append(path)
      for (x <- partition.split(",").indices) {
        stringBuilder.append("/*")
        x
      }
      stringBuilder.toString()
    }

    /**
      * @param path         hudi table save path
      * @param partitionKey partition keys
      * @return
      */
    def hudiSnapShot(path: String, partitionKey: String = ""): DataFrame = {

      reader.format("org.apache.hudi")
        .option(QUERY_TYPE_OPT_KEY, QUERY_TYPE_SNAPSHOT_OPT_VAL)
        .load(buildPartitionPath(path, partitionKey))
    }

    /**
      * @param path         hudi table path
      * @param partitionKey partition keys
      * @return
      */
    def hudiOptimized(path: String, partitionKey: String = ""): DataFrame = {
      reader.format("org.apache.hudi")
        .option(QUERY_TYPE_OPT_KEY, QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
        .load(buildPartitionPath(path, partitionKey))
    }

    /**
      * @param path            hudi table path
      * @param beginCommitTime start time, 14bit yyyyMMddHHmmss
      *                        example: '20170901080000' will get all new data written after Sep 1, 2017 08:00AM.
      * @param partitionPath   filter the partition path,  example: '/year=2020/month=*\/'
      * @param endCommitTime   end time , default value is latest commit time
      * @return
      */
    def hudiIncrement(
                       path: String,
                       beginCommitTime: String,
                       partitionPath: Option[String] = Option.empty,
                       endCommitTime: Option[String] = Option.empty
                     ): DataFrame = {
      reader.format("org.apache.hudi")
        .option(QUERY_TYPE_OPT_KEY, QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .option(BEGIN_INSTANTTIME_OPT_KEY, beginCommitTime)
        .option(INCR_PATH_GLOB_OPT_KEY, partitionPath.getOrElse(""))
        .option(END_INSTANTTIME_OPT_KEY, endCommitTime.getOrElse("END_INSTANTTIME"))
        .load(path)
    }
  }

}

