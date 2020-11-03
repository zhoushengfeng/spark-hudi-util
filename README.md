## Simple and quick start spark-hudi demo
#### start
```scala
    val spark = SparkSessionBuilder.createSparkSession("APP_NAME")
    import com.zhousf.hudi.util.HudiUtils._
    import spark.implicate._
```

#### easy to read 
```scala
    sparkSession.read.hudiSnapshot(params)
    sparkSession.read.hudiOptimized(params)
    sparkSession.read.hudiIncrement(params)
```    

#### easy to write
    
```scala
    dataframe
        .write
        .saveToHudi("upsert") // upsert insert bulk_insert delete
        //  .upsertToHudi
        //  .insertToHudi
        //  .bulkInsertToHudi
        //  .deleteFromHudi(true)
        .setHudiTableConfig(hudiTableConfig)
        .setHudiParallel(parallelConfig)
        .setHudiCompact(compactConfig)
        .save(path)
```

