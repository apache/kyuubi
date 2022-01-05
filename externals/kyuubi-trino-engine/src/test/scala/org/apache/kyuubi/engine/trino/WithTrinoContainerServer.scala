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

package org.apache.kyuubi.engine.trino

import java.net.URI
import java.time.ZoneId
import java.util.Locale
import java.util.Optional
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import io.airlift.units.Duration
import io.trino.client.ClientSelectedRole
import io.trino.client.ClientSession
import okhttp3.OkHttpClient
import org.testcontainers.containers.TrinoContainer

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf

trait WithTrinoContainerServer extends KyuubiFunSuite {

  final val IMAGE_VERSION = 363
  final val DOCKER_IMAGE_NAME = s"trinodb/trino:${IMAGE_VERSION}"

  val trino = new TrinoContainer(DOCKER_IMAGE_NAME)
  val kyuubiConf: KyuubiConf = KyuubiConf()

  protected val catalog = "tpch"
  protected val schema = "tiny"

  override def beforeAll(): Unit = {
    trino.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    trino.stop()
    super.afterAll()
  }

  lazy val connectionUrl = trino.getJdbcUrl.replace("jdbc:trino", "http")

  lazy val session = new ClientSession(
    URI.create(connectionUrl),
    "kyuubi_test",
    Optional.empty(),
    "kyuubi",
    Optional.empty(),
    Set[String]().asJava,
    null,
    catalog,
    schema,
    null,
    ZoneId.systemDefault(),
    Locale.getDefault,
    Map[String, String]().asJava,
    Map[String, String]().asJava,
    Map[String, String]().asJava,
    Map[String, ClientSelectedRole]().asJava,
    Map[String, String]().asJava,
    null,
    new Duration(2, TimeUnit.MINUTES),
    true)

  lazy val httpClient = new OkHttpClient.Builder().build()
}
