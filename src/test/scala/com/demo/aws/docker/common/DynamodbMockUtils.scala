package com.demo.aws.docker.common

import com.amazonaws.auth.{AWSStaticCredentialsProvider, AnonymousAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.model.{CreateTableRequest, CreateTableResult}
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import java.util.zip.GZIPInputStream

trait DynamodbMockUtils {
  def getClient(endpoint: EndpointConfiguration): AmazonDynamoDB =
    AmazonDynamoDBClientBuilder.standard
      .withEndpointConfiguration(endpoint)
      .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
      .build
  def createTable(client: AmazonDynamoDB, createTableRequest: CreateTableRequest): CreateTableResult = {
    client.createTable(createTableRequest)
  }
}
