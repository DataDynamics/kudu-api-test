package io.datadynamics.hskimsky

import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.util.LongAccumulator

import java.sql.Timestamp
import java.text.SimpleDateFormat
import scala.collection.JavaConverters._

/**
 * Starter
 *
 * @author Haneul, Kim
 * @version 0.1.0
 * @since 2022-04-13
 */
object Starter {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("yarn").setAppName("test")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    import spark.implicits._

    val tableName = "default.log_p78_hms"
    val tempViewName = "log_p78_hms_view"
    val kuduMaster = "adm1.dd.io:7051,hdm1.dd.io:7051,hdm2.dd.io:7051"
    val df: DataFrame = spark.read.options(Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> tableName
    )).format("kudu").load
    df.createOrReplaceTempView(tempViewName)
    // df.select("key").filter("key >= 5").show()
    val schema: StructType = df.schema

    // select * from default.log_p78_hms where start_time between '2021-01-01 00:00:00' and '2021-05-31 23:59:59'
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val df2: DataFrame = df.where($"start_time".between(
      new Timestamp(sdf.parse("2021-01-01 00:00:00").getTime + 9 * 3600000),
      new Timestamp(sdf.parse("2021-05-31 23:59:59").getTime + 9 * 3600000)
    ))
    val startTime: Long = System.nanoTime()
    val readAccum: LongAccumulator = spark.sparkContext.longAccumulator("read")
    df2.foreach(r => {
      readAccum.add(1)
    })
    val finishTime: Long = System.nanoTime()
    val elapsedTimeSeconds: Double = (finishTime - startTime).toDouble / 1000000000
    println(s"total rows = ${readAccum.value}")
    println(s"elapsedTime = ${elapsedTimeSeconds} secs")
    // expected: 93873513, actual: 93839960 ??

    df2.agg(
      min("start_time").as("min_start_time"),
      max("start_time").as("max_start_time")
    ).show(false)

    // df.createOrReplaceTempView("kudu_table")
    // spark.sql(s"select * from ${tempViewName} where start_time between '2021-01-01 00:00:00' and '2021-05-31 23:59:59'").show()
    spark.sql(s"select min(start_time) as min_start_time, max(start_time) as max_start_time from ${tempViewName} where start_time between '2021-01-01 00:00:00' and '2021-05-31 23:59:59'").show()
  }
}

class Starter {
  def example(): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("yarn").setAppName("test")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    import spark.implicits._

    val tableName = "default.log_p78_hms"
    val kuduMaster = "adm1.dd.io:7051,hdm1.dd.io:7051,hdm2.dd.io:7051"
    val df: DataFrame = spark.read.options(Map(
      "kudu.master" -> kuduMaster,
      "kudu.table" -> tableName
    )).format("kudu").load
    // df.select("key").filter("key >= 5").show()
    val schema: StructType = df.schema

    val kuduContext = new KuduContext(kuduMaster, spark.sparkContext)
    val kuduRdd: RDD[Row] = kuduContext.kuduRDD(spark.sparkContext, tableName)
    val kuduDF: DataFrame = spark.createDataFrame(kuduRdd, schema)
    kuduDF.show()

    val options: CreateTableOptions = new CreateTableOptions().setNumReplicas(1).addHashPartitions(List("key").asJava, 3)
    kuduContext.createTable("test_table", schema, Seq("key"), options)
    kuduContext.tableExists("test_table")
    kuduContext.insertRows(df, "test_table")
    kuduContext.deleteRows(df, "test_table")
    kuduContext.upsertRows(df, "test_table")

    val updateDF: DataFrame = df.select($"key", ($"int_val" + 1).as("int_val"))
    kuduContext.updateRows(updateDF, "test_table")
    df.write.options(Map("kudu.master" -> "kudu.master:7051", "kudu.table" -> "test_table")).mode("append").format("kudu").save
    // Delete a Kudu table
    kuduContext.deleteTable("test_table")
  }
}