package com.zhousf.hudi.demo

import com.zhousf.hudi.util.SparkSessionBuilder
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.junit.{Before, Test}
import com.zhousf.hudi.util.HudiUtils._

import scala.io.Source
import scala.util.Random


/**
  * ${description}
  **/
class HudiDemo {


  var spark: SparkSession = _

  val testFilePath = "test_file\\test_data.txt"
  var resourcePath: String = "D:\\WorkSpace\\weiboyi\\HudiDemos\\src\\main\\resources\\"


  val hudiTableName = "ods.hudi_demo"

  val recordKey = "id"
  val partitionKey = "class,gender"
  val combineKey = "modify_at"


  @Before def init: Unit = {
    spark = SparkSessionBuilder.createSparkSession(getClass.getSimpleName)
  }


  @Test def buildTestData: Unit = {
    import java.io.PrintWriter
    // schema : id name class gender score
    val out = new PrintWriter(resourcePath + testFilePath)
    for (i <- 1 to 100) out.println(
      i + "@@" + Random.nextDouble() + "@@" + Random.nextInt(5) + "@@" + {
        if (Random.nextInt(2) == 0) "female" else "male"
      } + "@@" + Random.nextInt(100)
    )
    out.close()
    println(s"Successfully generated the test data!")
  }


  @Test def writeDataToHudi = {

    val testFileDF: DataFrame = spark.createDataFrame(
      spark.sparkContext.textFile(resourcePath + testFilePath)
        .map(x => x.split("@@"))
        .map(ele => {
          try {
            (ele(0).toInt, ele(1), ele(2), ele(3), ele(4).toInt)
          } catch {
            case e: Exception => println(ele.mkString(" || "))
          }
          (ele(0).toInt, ele(1), ele(2), ele(3), ele(4).toInt)
        })
    ).toDF("id", "name", "class", "gender", "score")


    testFileDF
      .withColumn(combineKey, functions.lit(System.currentTimeMillis()))
      .write
      .saveToHudi("upsert") // upsert insert bulk_insert delete
      //  .upsertToHudi
      //  .insertToHudi
      //  .bulkInsertToHudi
      //  .deleteFromHudi(true)
      .setHudiTableConfig(hudiTableName, combineKey, recordKey, partitionKey, tableType = "MERGE_ON_READ")
      .setHudiParallel(4, 4)
      .setHudiCompact(retainedCommit = 1, retainedFileVersion = 1)
      .save(resourcePath + hudiTableName)

  }


  @Test def readDataFromHudi = {

    val snapshotDF = spark.read.hudiSnapShot(resourcePath + hudiTableName, partitionKey)
    snapshotDF.printSchema()
    snapshotDF.show(false)

    val optimizedDF = spark.read.hudiOptimized(resourcePath + hudiTableName, partitionKey)
    optimizedDF.printSchema()
    optimizedDF.show(false)

    // Incremental view not implemented yet, for merge-on-read tables
    val incrementDF = spark.read.hudiIncrement(resourcePath + hudiTableName, "20201026120000")
    incrementDF.printSchema()
    incrementDF.show(false)

  }


}
