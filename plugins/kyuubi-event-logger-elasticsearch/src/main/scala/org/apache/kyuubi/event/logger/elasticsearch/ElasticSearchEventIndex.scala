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

package org.apache.kyuubi.event.logger.elasticsearch

import java.io.Closeable
import java.util.Locale

import com.google.common.annotations.VisibleForTesting

import org.apache.kyuubi.Logging
import org.apache.kyuubi.event.logger.elasticsearch.ElasticSearchEventIndex._
import org.apache.kyuubi.events.KyuubiEvent

class ElasticSearchEventIndex(client: ElasticSearchClient, indices: Option[String])
  extends Closeable with Logging {

  def index(event: KyuubiEvent): Unit = {
    client.index(getIndex(event.eventType), event.toJson)
  }

  private val (index, indexMap): (Option[String], Map[String, String]) = {
    if (indices.isDefined) {
      val arr = indices.get.split(",").map(_.split(":"))
      (arr.filter(_.length == 1).map(_(0)).headOption,
        arr.filter(_.length == 2).map(a => (a(0).toUpperCase(Locale.ROOT), a(1))).toMap)
    } else {
      (None, Map.empty)
    }
  }

  def createIndices(): Unit = {
    index match {
      case Some(i) => createIndex(i)
      case _ => createIndex(DEFAULT_EVENT_INDEX)
    }

    indexMap.values.foreach(createIndex)
  }

  private def createIndex(indexName: String): Unit = {
    client.createIndex(indexName)
  }

  @VisibleForTesting
  private[elasticsearch] def getIndex(eventType: String): String = {
    val eventIndex = indexMap.get(eventType.toUpperCase(Locale.ROOT))
    if (eventIndex.isDefined) {
      eventIndex.get
    } else {
      index.getOrElse(DEFAULT_EVENT_INDEX)
    }
  }

  override def close(): Unit = {
    try {
      client.close()
    } catch {
      case e: Exception =>
        warn("ElasticSearchEventIndex close error.", e)
    }
  }
}

object ElasticSearchEventIndex {

  val DEFAULT_EVENT_INDEX = "kyuubi-events"

  def apply(hosts: String,
            username: Option[String],
            password: Option[String],
            indices: Option[String]): ElasticSearchEventIndex = {
    val client = new ElasticSearchClientBuilder()
      .hosts(hosts)
      .username(username)
      .password(password)
      .build()
    new ElasticSearchEventIndex(client, indices)
  }

}
