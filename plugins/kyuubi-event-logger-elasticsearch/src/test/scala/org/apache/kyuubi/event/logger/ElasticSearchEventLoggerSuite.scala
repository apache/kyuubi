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

package org.apache.kyuubi.event.logger

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.Request

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.event.logger.ElasticSearchEventLoggerConf._
import org.apache.kyuubi.events.KyuubiEvent

class ElasticSearchEventLoggerSuite extends WithElasticsearchContainer {

  val conf = KyuubiConf()

  test("log event into elasticsearch") {
    val index = "kyuubi-test-events"
    val logger = new ElasticSearchEventLogger[KyuubiTestEvent]
    conf.set(ELASTICSEARCH_EVENT_LOGGER_HOSTS, getElasticSearchHosts())
    conf.set(ELASTICSEARCH_EVENT_LOGGER_INDICES, index)
    logger.initialize(conf)

    val event = KyuubiTestEvent("001", "test001")
    logger.logEvent(event)

    assert(event == getKyuubiTestEvent(index, "001"))
  }

  private def getKyuubiTestEvent(index: String, id: String): KyuubiEvent = {
    restClient.performRequest(new Request("POST", s"$index/_refresh"))
    val request = new Request("GET", s"$index/_search")
    request.setJsonEntity(s"""{"query":{"term":{"id":"$id"}}}""")
    val response = restClient.performRequest(request)
    val result = EntityUtils.toString(response.getEntity)
    val mapper = new ObjectMapper()
    val json = mapper.readTree(result)
    json.at("/hits/hits") match {
      case hits: ArrayNode =>
        val source = hits.get(0).get("_source")
        KyuubiTestEvent(source.get("id").asText(), source.get("value").asText())
      case _ => null
    }
  }

}

private case class KyuubiTestEvent(id: String, value: String) extends KyuubiEvent {

  override def partitions: Seq[(String, String)] = Seq.empty

}
