package com.demo.aws.docker.common

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import scala.io.Source

trait LocalSparkSession extends Serializable {
  implicit val sparkSession: SparkSession = {
    val ss = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.hadoop.mapreduce.map.memory.mb", "5012")
      .appName("appName")
      .getOrCreate()
    ss.sparkContext.setLogLevel("WARN")
    ss
  }
  def readFs[T: Encoder](path: String, readFormat: String = "delta"): Dataset[T] = {
    import sparkSession.implicits._
    Seq.empty[T].toDF().unionByName(sparkSession.read.format(readFormat).load(path), allowMissingColumns = true).as[T]
  }

  def readFile(st: String): String = {
    val bufferedSource = Source.fromFile(st)
    bufferedSource.mkString.trim
  }
}
