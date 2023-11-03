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

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.Failure

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
import com.sksamuel.elastic4s.ElasticApi.indexInto
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.requests.common.RefreshPolicy
import com.sksamuel.elastic4s.requests.mappings.MappingDefinition
import com.sksamuel.elastic4s.requests.mappings.dynamictemplate.DynamicMapping
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.util.Preconditions.checkArgument
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.events.KyuubiEvent
import org.apache.kyuubi.events.handler.ElasticSearchLoggingEventHandler.getElasticClient
import org.apache.kyuubi.util.ThreadUtils

/**
 * This event logger logs events to ElasticSearch.
 */
class ElasticSearchLoggingEventHandler(
    indexId: String,
    serverUrl: String,
    username: Option[String] = None,
    password: Option[String] = None,
    kyuubiConf: KyuubiConf) extends EventHandler[KyuubiEvent] with Logging {

  private val isAutoCreateIndex: Boolean =
    kyuubiConf.get(SERVER_EVENT_ELASTICSEARCH_INDEX_AUTOCREATE_ENABLED)

  private val esClient: ElasticClient = getElasticClient(serverUrl, username, password)

  private val executorService =
    ThreadUtils.newDaemonFixedThreadPool(1, "elasticsearch-event-handler-execution")
  implicit private val executionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(executorService)

  private def checkIndexExisted: String => Boolean = (indexId: String) => {
    checkArgument(
      StringUtils.isNotBlank(indexId),
      "index name must be configured for ElasticSearchEventHandler, please ensure %s is set",
      SERVER_EVENT_ELASTICSEARCH_INDEX.key)
    // check
    esClient.execute {
      indexExists(indexId)
    }.await.result.isExists
  }

  private def checkAndEnsureIndex(indexId: String): Unit = {
    if (!checkIndexExisted(indexId) && isAutoCreateIndex) {
      esClient.execute {
        createIndex(indexId)
          .mapping(MappingDefinition(dynamic = Some(DynamicMapping.Dynamic)))
      }.await
      if (!checkIndexExisted(indexId)) {
        throwExceptionIndexNotfound(indexId)
      }
    } else {
      throwExceptionIndexNotfound(indexId)
    }
  }

  checkAndEnsureIndex(indexId)

  private val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  override def apply(event: KyuubiEvent): Unit = {
    val fields = {
      objectMapper.readValue(event.toJson, classOf[Map[String, Any]]).map {
        case (key: String, value: Any) =>
          val isNumber = (obj: Any) => {
            obj.isInstanceOf[Byte] || obj.isInstanceOf[Short] ||
            obj.isInstanceOf[Int] || obj.isInstanceOf[Long] ||
            obj.isInstanceOf[Float] || obj.isInstanceOf[Double]
          }
          if (value.isInstanceOf[String] || isNumber(value)) {
            (key, value)
          } else {
            (key, objectMapper.writeValueAsString(value))
          }
      }
    }
    esClient.execute {
      indexInto(indexId).fields(fields).refresh(RefreshPolicy.Immediate)
    }.onComplete {
      case Failure(f) =>
        error(s"Failed to send event in ElasticSearchLoggingEventHandler, ${f.getMessage}", f)
      case _ =>
    }
  }

  override def close(): Unit = {
    executorService.shutdown()
    esClient.close()
  }

  private def throwExceptionIndexNotfound(indexId: String) =
    throw new KyuubiException(s"the index '$indexId' is not found on ElasticSearch")
}

object ElasticSearchLoggingEventHandler {
  def getElasticClient(
      serverUrl: String,
      user: Option[String],
      password: Option[String]): ElasticClient = {
    val props = ElasticProperties(serverUrl)
    val configCallback: HttpClientConfigCallback =
      (httpAsyncClientBuilder: HttpAsyncClientBuilder) => {
        (user, password) match {
          case (Some(userStr), Some(passwordStr)) =>
            val provider = new BasicCredentialsProvider
            val credentials = new UsernamePasswordCredentials(userStr, passwordStr)
            provider.setCredentials(AuthScope.ANY, credentials)
            httpAsyncClientBuilder.setDefaultCredentialsProvider(provider)
          case _ => httpAsyncClientBuilder
        }
      }
    val javaClient = JavaClient.apply(props, configCallback)
    ElasticClient(javaClient)
  }
}
