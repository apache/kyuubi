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

import java.util.UUID

import com.dimafeng.testcontainers.ElasticsearchContainer
import com.dimafeng.testcontainers.ElasticsearchContainer.defaultImage
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import com.sksamuel.elastic4s.ElasticDsl._

import org.apache.kyuubi._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.events.handler.ElasticSearchLoggingEventHandler.getElasticClient
import org.apache.kyuubi.operation.HiveJDBCTestHelper

abstract class ServerElasticSearchLoggingEventHandlerSuite extends WithKyuubiServer
  with HiveJDBCTestHelper
  with BatchTestHelper with TestContainerForAll {

  protected val imageTag: String
  override lazy val containerDef = ElasticsearchContainer.Def(s"$defaultImage:$imageTag")

  override def startContainers(): containerDef.Container = {
    val esContainer = containerDef.createContainer()
    val jContainer = esContainer.container
    jContainer.withPassword(esPassword)
    jContainer.withEnv("xpack.security.enabled", "true")
    esContainer.start()
    esContainer
  }

  private val destIndex = "server-event-index"
  override protected def jdbcUrl: String = getJdbcUrl

  private var esServerUrl: String = _
  private val esUser = "elastic"
  private val esPassword = UUID.randomUUID().toString

  override protected val conf: KyuubiConf = {
    KyuubiConf()
      .set(KyuubiConf.SERVER_EVENT_LOGGERS, Seq("ELASTICSEARCH"))
      .set(KyuubiConf.SERVER_EVENT_ELASTICSEARCH_INDEX, destIndex)
      .set(KyuubiConf.SERVER_EVENT_ELASTICSEARCH_INDEX_AUTOCREATE_ENABLED, true)
      .set(KyuubiConf.SERVER_EVENT_ELASTICSEARCH_USER, esUser)
      .set(KyuubiConf.SERVER_EVENT_ELASTICSEARCH_PASSWORD, esPassword)
  }

  override def beforeAll(): Unit = withContainers { esContainer =>
    esServerUrl = s"http://${esContainer.httpHostAddress}"
    conf.set(SERVER_EVENT_ELASTICSEARCH_SERVER_URL, esServerUrl)
    super.beforeAll()
  }

  test("check server events sent to ElasticSearch index") {
    withContainers { _ =>
      val esClient = getElasticClient(esServerUrl, Some(esUser), Some(esPassword))
      try {
        Thread.sleep(1000)
        val hits = esClient.execute {
          search(destIndex).matchAllQuery()
        }.await.result.hits.hits
        assert(hits.length > 0)
      } finally {
        esClient.close()
      }
    }
  }

}

class ServerElasticSearchLoggingEventHandlerSuiteForEs7
  extends ServerElasticSearchLoggingEventHandlerSuite {
  override val imageTag = "7.17.14"
}

class ServerElasticSearchLoggingEventHandlerSuiteForEs8
  extends ServerElasticSearchLoggingEventHandlerSuite {
  override val imageTag = "8.10.4"
}
