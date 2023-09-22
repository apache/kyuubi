/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.events.handler

import java.time.Duration
import java.util.Properties

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Random

import com.dimafeng.testcontainers.KafkaContainer
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import com.fasterxml.jackson.databind.json.JsonMapper
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.consumer.KafkaConsumer

import org.apache.kyuubi._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.events.handler.ServerKafkaLoggingEventHandler.KAFKA_SERVER_EVENT_HANDLER_PREFIX
import org.apache.kyuubi.operation.HiveJDBCTestHelper

abstract class ServerKafkaLoggingEventHandlerSuite extends WithKyuubiServer with HiveJDBCTestHelper
  with BatchTestHelper with TestContainerForAll {

  /**
   * `confluentinc/cp-kafka` is Confluent Community Docker Image for Apache Kafka.
   * The list of compatibility for Kafka's version refers to:
   * https://docs.confluent.io/platform/current/installation
   * /versions-interoperability.html#cp-and-apache-ak-compatibility
   */
  protected val imageTag: String
  override lazy val containerDef: KafkaContainer.Def =
    KafkaContainer.Def(s"confluentinc/cp-kafka:$imageTag")
  private val destTopic = "server-event-topic"
  private val mapper = JsonMapper.builder().build()
  override protected def jdbcUrl: String = getJdbcUrl

  override protected val conf: KyuubiConf = {
    KyuubiConf()
      .set(KyuubiConf.SERVER_EVENT_LOGGERS, Seq("KAFKA"))
      .set(KyuubiConf.SERVER_EVENT_KAFKA_TOPIC, destTopic)
  }

  override def beforeAll(): Unit = withContainers { kafkaContainer =>
    val bootstrapServers = kafkaContainer.bootstrapServers
    createTopic(kafkaContainer.bootstrapServers, destTopic)
    conf.set(s"$KAFKA_SERVER_EVENT_HANDLER_PREFIX.bootstrap.servers", bootstrapServers)

    super.beforeAll()
  }

  private def createTopic(kafkaServerUrl: String, topic: String): Unit = {
    val adminProps = new Properties
    adminProps.setProperty("bootstrap.servers", kafkaServerUrl)
    val adminClient = AdminClient.create(adminProps)
    adminClient.createTopics(List(new NewTopic(topic, 1, 1.toShort)).asJava)
    adminClient.close()
  }

  test("check server events sent to kafka topic") {
    withContainers { kafkaContainer =>
      val consumerConf = new Properties
      Map(
        "bootstrap.servers" -> kafkaContainer.bootstrapServers,
        "group.id" -> s"server-kafka-logger-test-${Random.nextInt}",
        "auto.offset.reset" -> "earliest",
        "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
        "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")
        .foreach(p => consumerConf.setProperty(p._1, p._2))
      val consumer = new KafkaConsumer[String, String](consumerConf)
      try {
        consumer.subscribe(List(destTopic).asJava)
        eventually(timeout(10.seconds), interval(500.milliseconds)) {
          val records = consumer.poll(Duration.ofMillis(500))
          assert(records.count() > 0)
          records.forEach { record =>
            val jsonObj = mapper.readTree(record.value())
            assertResult("kyuubi_server_info")(record.key)
            assertResult(server.getName)(jsonObj.get("serverName").asText())
          }
        }
      } finally {
        consumer.close()
      }
    }
  }
}

class ServerKafkaLoggingEventHandlerSuiteForKafka2 extends ServerKafkaLoggingEventHandlerSuite {
  // equivalent to Apache Kafka 2.8.x
  override val imageTag = "6.2.10"
}

class ServerKafkaLoggingEventHandlerSuiteForKafka3 extends ServerKafkaLoggingEventHandlerSuite {
  // equivalent to Apache Kafka 3.3.x
  override val imageTag = "7.3.3"
}
