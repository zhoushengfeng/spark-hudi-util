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
      * @param hiveDataBaseName                hive数据库名
      * @param hiveTableName                   hive表名
      * @param hivePartitionFields             hive分区字段
      * @param hivePartitionExtractorClassName 分区提取字段，本配置下默认hudi多分区字段分割符为反斜线
      * @param hiveUserName                    hive 用户名
      * @param hivePassword                    hive 密码
      * @param hiveServer2URL                  connect to hive : jdbc:hive2://host:10000
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
      * @param upsertParallel 更新并行度，默认 1500
      * @param insertParallel 插入并行度，默认 1500
      * @return
      */
    def setHudiParallel(upsertParallel: Int = 1500, insertParallel: Int = 1500): DataFrameWriter[T] = {
      hudiConfig
        .option("hoodie.upsert.shuffle.parallelism", upsertParallel)
        .option("hoodie.insert.shuffle.parallelism", insertParallel)
    }

    /**
      *
      * @param cleanPolicy         清理策略，基于提交会文件版本，增量读取场景建议使用 KEEP_LATEST_COMMIT ，其他场景 KEEP_LATEST_FILE_VERSIONS
      * @param asyncClean          异步清理
      * @param inlineCompact       内联压缩
      * @param minCommits          保留最小提交数
      * @param maxCommits          保留最大提交数
      * @param retainedCommit      清理保存的提交数
      * @param retainedFileVersion 清理保留的文件版本数
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
        .option("hoodie.cleaner.commits.retained", retainedCommit) // 提交保留 默认 10
        .option("hoodie.keep.min.commits", minCommits) // 保存最小提交数 默认 20
        .option("hoodie.keep.max.commits", maxCommits) // 保存最大提交数 默认 30
        .option("hoodie.cleaner.policy", cleanPolicy) // 文件保存策略
        .option("hoodie.cleaner.fileversions.retained", retainedFileVersion) // 文件版本保留 默认 3
        .option("hoodie.cleaner.parallelism", "200") // 默认 200
        .option("hoodie.clean.automatic", "true") // 自动清理 默认true
        .option("hoodie.clean.async", asyncClean) // 异步清理 默认 false
        .option("hoodie.compact.inline", inlineCompact) // 内联压缩 默认 false  降低性能
        .option("hoodie.compact.inline.max.delta.commits", "5") // 内联 最大压缩值 默认 5
        .option("hoodie.commits.archival.batch", minCommits / 2) // 归档数量 默认10
    }

    /**
      * @param tableName             hudi表名
      * @param preCombineFields      合并时比较的字段 example:modify_time
      * @param recordKeyFields       主键 unique, 多字段逗号分割 example: "timestamp,id"
      * @param partitionPathFields   分区字段 ，多字段逗号分割 example: "ds,platform_type"
      * @param tableType             hudi表类型，默认 merge_on_read
      * @param keyGeneratorClassName 键值（分区字段）提取器  default = ComplexKeyGenerator
      * @param hiveStylePartitioning hive分区格式  partitioin=xxx,default = true
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
        .option(TABLE_TYPE_OPT_KEY, tableType) // merage_on_read  cope_on_write
        // 当前数据的分区变更时，数据的分区目录也变化 全局GLOBAL_BLOOM
        .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
        .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name())

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
      * @param isHardDeletes 删除时可选项，默认true  硬删除
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
            classOf[EmptyHoodieRecordPayload].getName // 软删除 保留键值
          else
            classOf[OverwriteWithLatestAvroPayload].getName) // 硬删除
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
      }
      stringBuilder.toString()
    }

    /**
      * @param path         hudi表路径
      * @param partitionKey 分区字段，多字段分区逗号分割,默认分区或单字段分区时不需要配置
      * @return
      */
    def hudiSnapShot(path: String, partitionKey: String = ""): DataFrame = {

      reader.format("org.apache.hudi")
        .option(QUERY_TYPE_OPT_KEY, QUERY_TYPE_SNAPSHOT_OPT_VAL)
        .load(buildPartitionPath(path, partitionKey))
    }

    /**
      * @param path         hudi表路径
      * @param partitionKey 分区字段，多字段分区逗号分割,默认分区或单字段分区时不需要配置
      * @return
      */
    def hudiOptimized(path: String, partitionKey: String = ""): DataFrame = {
      reader.format("org.apache.hudi")
        .option(QUERY_TYPE_OPT_KEY, QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
        .load(buildPartitionPath(path, partitionKey))
    }

    /**
      * @param path            hudi表路径
      * @param beginCommitTime 起始时间轴,14位，年月日时分秒 - yyyyMMddHHmmss
      *                        example: '20170901080000' will get all new data written after Sep 1, 2017 08:00AM.
      * @param partitionPath   从指定的分区增量拉取,多路径使用反斜线分割，*代表全匹配  example: '/year=2020/month=*\/'
      * @param endCommitTime   结束时间轴，默认最新提交时间
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

