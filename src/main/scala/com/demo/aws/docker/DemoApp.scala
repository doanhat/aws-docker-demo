package com.demo.aws.docker

import com.demo.aws.docker.config.DemoConfig
import com.demo.aws.docker.processor.DemoProcessor
import pureconfig.ConfigSource
import pureconfig.generic.auto.exportReader

case object DemoApp extends ClusterSparkSession with Serializable {
  def main(args: Array[String]): Unit = {
    implicit val config: DemoConfig = ConfigSource.default.at("demo").loadOrThrow[DemoConfig]
    val processor: DemoProcessor = new DemoProcessor()
    processor.process()
  }
}
