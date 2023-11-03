package com.demo.aws.docker

import org.apache.spark.sql.SparkSession

trait ClusterSparkSession extends Serializable {
  implicit val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("demo")
    .getOrCreate()
}
