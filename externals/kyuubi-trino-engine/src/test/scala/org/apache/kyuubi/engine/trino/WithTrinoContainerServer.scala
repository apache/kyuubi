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

import com.dimafeng.testcontainers.TrinoContainer
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import io.airlift.units.Duration
import io.trino.client.ClientSession
import okhttp3.OkHttpClient
import org.testcontainers.utility.DockerImageName

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf

trait WithTrinoContainerServer extends KyuubiFunSuite with TestContainerForAll {

  final val IMAGE_VERSION = 411
  final val DOCKER_IMAGE_NAME = s"trinodb/trino:$IMAGE_VERSION"

  override val containerDef = TrinoContainer.Def(DockerImageName.parse(DOCKER_IMAGE_NAME))

  val kyuubiConf: KyuubiConf = KyuubiConf()

  protected val catalog = "tpch"
  protected val schema = "tiny"

  def withTrinoContainer(tc: TrinoContext => Unit): Unit = {
    withContainers { trinoContainer =>
      val connectionUrl = trinoContainer.jdbcUrl.replace("jdbc:trino", "http")
      val trinoContext = TrinoContext(httpClient, session(connectionUrl))
      tc(trinoContext)
    }
  }

  protected def session(connectionUrl: String): ClientSession = ClientSession.builder()
    .server(URI.create(connectionUrl))
    .principal(Optional.of("kyuubi_test"))
    .source("kyuubi")
    .catalog(catalog)
    .schema(schema)
    .timeZone(ZoneId.systemDefault())
    .locale(Locale.getDefault)
    .clientRequestTimeout(new Duration(2, TimeUnit.MINUTES))
    .compressionDisabled(true)
    .build()

  lazy val httpClient = new OkHttpClient.Builder().build()
}
