package com.demo.aws.docker.processor

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.demo.aws.docker.InputData
import com.demo.aws.docker.config.DemoConfig
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession}

import java.util
import scala.collection.JavaConverters.mapAsJavaMapConverter

class DemoProcessor(
                     implicit val sparkSession: SparkSession, implicit val config: DemoConfig
                   ) extends Serializable {
  implicit val inputData: Encoder[InputData] = ExpressionEncoder()
  val dynamoDBEndpoint: String = "dynamodb.eu-west-1.amazonaws.com"
  val dynamoDBRegion: String = "eu-west-1"

  private def read(path: String): Dataset[InputData] = sparkSession.read.format("json").load(path).as[InputData]

  def getJobConf(
                  sc: SparkContext,
                  endpoint: String = dynamoDBEndpoint,
                  region: String = dynamoDBRegion
                ): JobConf = {
    val jobConf = new JobConf(sc.hadoopConfiguration)
    jobConf.set("dynamodb.input.tableName", config.dest.output.path)
    jobConf.set("dynamodb.output.tableName", config.dest.output.path)
    jobConf.set("dynamodb.endpoint", endpoint)
    jobConf.set("dynamodb.regionid", region)
    jobConf.set("mapred.output.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat")
    jobConf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat")
    jobConf
  }

  private def getAttributesMap(row: Row): util.Map[String, AttributeValue] = {
    Map(
      "id" -> new AttributeValue().withS(row.getAs[String]("id")),
      "doubledPoint" -> new AttributeValue().withN(row.getAs[Long]("doubledPoint").toString)
    ).asJava
  }

  def write(ds: DataFrame): Unit = {
    val jobConf = getJobConf(sparkSession.sparkContext)
    val rdd: RDD[(Text, DynamoDBItemWritable)] = ds.rdd.mapPartitions(partition => {
      partition.map(row => {
        val item = new DynamoDBItemWritable()
        item.setItem(getAttributesMap(row))
        (new Text(""), item)
      })
    })
    rdd.saveAsHadoopDataset(jobConf)
  }

  def process(): Unit = {
    val inputDs = read(config.source.input.path)
    val df: DataFrame = inputDs.withColumn("doubledPoint", (col("point") * 2))
    write(df)
  }
}
