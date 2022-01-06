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

import org.apache.http.HttpHost
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.CredentialsProvider
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.client.{Request, RestClient}
import org.elasticsearch.client.sniff.Sniffer

import org.apache.kyuubi.Logging

class ElasticSearchClient(client: RestClient, sniffer: Sniffer) extends Closeable with Logging {

  def index(index: String, json: String): Unit = {
    val request = new Request("POST", s"$index/_doc")
    request.setJsonEntity(json)
    client.performRequest(request)
  }

  def createIndex(index: String): Unit = {
    val request = new Request("PUT", s"$index")
    try {
      client.performRequest(request)
    } catch {
      case e: Exception =>
        info(s"Failed to create index, ignore it. error message: ${e.getMessage}")
    }
  }

  override def close(): Unit = {
    sniffer.close()
    client.close()
  }

}

class ElasticSearchClientBuilder() {

  private var hosts: Array[HttpHost] = Array()
  private var username: Option[String] = None
  private var password: Option[String] = None

  def hosts(hosts: String): this.type = {
    this.hosts = hosts.split(",")
      .map(h => h.split(":"))
      .map(a => if (a.length < 2) {
        new HttpHost(a(0), 9200)
      } else {
        new HttpHost(a(0), a(1).toInt)
      })
    this
  }

  def username(username: String): this.type = {
    this.username = Some(username)
    this
  }

  def username(username: Option[String]): this.type = {
    this.username = username
    this
  }

  def password(password: String): this.type = {
    this.password = Some(password)
    this
  }

  def password(password: Option[String]): this.type = {
    this.password = password
    this
  }

  private def credentialsProvider(): Option[CredentialsProvider] = {
    if (username.isDefined && password.isDefined) {
      val credentialsProvider = new BasicCredentialsProvider
      credentialsProvider.setCredentials(AuthScope.ANY,
        new UsernamePasswordCredentials(username.get, password.get))
      Some(credentialsProvider)
    } else {
      None
    }
  }

  private def restClient(): RestClient = {
    val restClientBuilder = RestClient.builder(this.hosts: _*)
    credentialsProvider.map(c => {
      restClientBuilder.setHttpClientConfigCallback((httpClientBuilder: HttpAsyncClientBuilder) => {
        httpClientBuilder.setDefaultCredentialsProvider(c)
      }).build()
    })
    restClientBuilder.build()
  }

  private def sniffer(): Sniffer = Sniffer.builder(restClient).build

  def build(): ElasticSearchClient = new ElasticSearchClient(restClient, sniffer)

}