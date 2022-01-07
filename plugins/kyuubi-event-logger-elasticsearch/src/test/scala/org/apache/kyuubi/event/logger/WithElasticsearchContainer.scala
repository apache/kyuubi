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

import org.elasticsearch.client.RestClient
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.utility.DockerImageName

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.event.logger.WithElasticsearchContainer._
import org.apache.kyuubi.event.logger.elasticsearch.{ElasticSearchClient, ElasticSearchClientBuilder}

trait WithElasticsearchContainer extends KyuubiFunSuite {

  private var container: ElasticsearchContainer = _

  private var client: ElasticSearchClient = _

  override def beforeAll(): Unit = {
    container = new ElasticsearchContainer(ELASTICSEARCH_IMAGE)
    container.start()
    client = new ElasticSearchClientBuilder()
      .hosts(getElasticSearchHosts())
      .build()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if (client != null) {
      client.close()
    }
    if (container != null) {
      container.stop()
    }
  }

  def restClient(): RestClient = client.getClient()

  def getElasticSearchHosts(): String = container.getHttpHostAddress

}

object WithElasticsearchContainer {

  val ELASTICSEARCH_VERSION = "7.11.2"

  val ELASTICSEARCH_IMAGE = DockerImageName
    .parse("docker.elastic.co/elasticsearch/elasticsearch")
    .withTag(ELASTICSEARCH_VERSION)

}
