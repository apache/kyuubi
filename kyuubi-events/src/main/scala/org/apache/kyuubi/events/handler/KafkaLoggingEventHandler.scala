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

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.events.KyuubiEvent
import org.apache.kyuubi.events.handler.KafkaLoggingEventHandler._

/**
 * This event logger logs events to Kafka.
 */
class KafkaLoggingEventHandler(
    topic: String,
    producerConf: Iterable[(String, String)],
    kyuubiConf: KyuubiConf,
    closeTimeoutInMs: Long) extends EventHandler[KyuubiEvent] with Logging {
  private def defaultProducerConf: Properties = {
    val conf = new Properties()
    conf.setProperty("key.serializer", DEFAULT_SERIALIZER_CLASS)
    conf.setProperty("value.serializer", DEFAULT_SERIALIZER_CLASS)
    conf
  }

  private val normalizedProducerConf: Properties = {
    val conf = defaultProducerConf
    producerConf.foreach(p => conf.setProperty(p._1, p._2))
    conf
  }

  private val kafkaProducer = new KafkaProducer[String, String](normalizedProducerConf)

  override def apply(event: KyuubiEvent): Unit = {
    try {
      val record = new ProducerRecord[String, String](topic, event.eventType, event.toJson)
      kafkaProducer.send(record)
    } catch {
      case e: Exception =>
        error("Failed to send event in KafkaEventHandler", e)
    }
  }

  override def close(): Unit = {
    kafkaProducer.close(Duration.ofMillis(closeTimeoutInMs))
  }
}

object KafkaLoggingEventHandler {
  private val DEFAULT_SERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringSerializer"
}
