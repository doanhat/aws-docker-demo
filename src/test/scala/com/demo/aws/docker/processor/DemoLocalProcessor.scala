package com.demo.aws.docker.processor

import com.demo.aws.docker.config.DemoConfig
import org.apache.spark.sql.SparkSession

class DemoLocalProcessor(endpoint: String, region: String)
                        (implicit override val sparkSession: SparkSession, override implicit val config: DemoConfig)
  extends DemoProcessor {
  override val dynamoDBEndpoint: String = endpoint
  override val dynamoDBRegion: String = region
}
