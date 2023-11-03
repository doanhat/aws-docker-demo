package com.demo.aws.docker.processor

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.model._
import com.demo.aws.docker.common.{DynamodbMockUtils, LocalSparkSession}
import com.demo.aws.docker.config.DemoConfig
import com.dimafeng.testcontainers.LocalStackV2Container
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import org.scalatest.{BeforeAndAfterAll, FlatSpec, GivenWhenThen, Matchers}
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import pureconfig.ConfigSource
import pureconfig.generic.auto.exportReader

import scala.collection.JavaConverters.mapAsJavaMapConverter

case class DemoProcessorTest() extends FlatSpec
  with BeforeAndAfterAll
  with Matchers
  with GivenWhenThen
  with TestContainerForAll
  with DynamodbMockUtils
  with LocalSparkSession {

  implicit val config: DemoConfig = ConfigSource.default.at("demo").loadOrThrow[DemoConfig]

  @transient override val containerDef: LocalStackV2Container.Def =
    LocalStackV2Container.Def(
      tag = "1.3.0",
      services = Seq(Service.DYNAMODB)
    )

  "My Demo processor" should "act correctly" in {
    withContainers { ls => {
      val dynamodbClient =
        getClient(new EndpointConfiguration(ls.endpointOverride(Service.DYNAMODB).toString, ls.region.toString))

      createTable(dynamodbClient, createTableRequest)

      val localProcessor = new DemoLocalProcessor(ls.endpointOverride(Service.DYNAMODB).toString,ls.region.toString)
      localProcessor.process()

      val respondedResult: String = dynamodbClient.getItem(getItemRequest).getItem.get("doubledPoint").getN

      respondedResult shouldBe "20"
    }}
  }

  val createTableRequest: CreateTableRequest = new CreateTableRequest()
    .withTableName(config.dest.output.path) // Replace with your table name
    .withKeySchema(
      new KeySchemaElement()
        .withAttributeName("id")
        .withKeyType(KeyType.HASH)
    )
    .withAttributeDefinitions(
      new AttributeDefinition()
        .withAttributeName("id")
        .withAttributeType(ScalarAttributeType.S)
    )
    .withProvisionedThroughput(
      new ProvisionedThroughput()
        .withReadCapacityUnits(5L)
        .withWriteCapacityUnits(5L)
    )

  val getItemRequest: GetItemRequest = new GetItemRequest()
    .withTableName(config.dest.output.path)
    .withKey(
      Map(
        "id" -> new AttributeValue().withS("a")
      ).asJava
    )
}
