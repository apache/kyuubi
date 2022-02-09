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

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf

trait WithTrinoEngine extends KyuubiFunSuite with WithTrinoContainerServer {

  protected var engine: TrinoSqlEngine = _
  protected var connectionUrl: String = _

  override val kyuubiConf: KyuubiConf = TrinoSqlEngine.kyuubiConf

  def withKyuubiConf: Map[String, String]

  override def beforeAll(): Unit = {
    withContainers { trinoContainer =>
      val containerConnectionUrl = trinoContainer.jdbcUrl.replace("jdbc:trino", "http")
      startTrinoEngine(containerConnectionUrl)
      super.beforeAll()
    }
  }

  def startTrinoEngine(containerConnectionUrl: String): Unit = {
    kyuubiConf.set(KyuubiConf.ENGINE_TRINO_CONNECTION_URL, containerConnectionUrl)

    withKyuubiConf.foreach { case (k, v) =>
      System.setProperty(k, v)
      kyuubiConf.set(k, v)
    }

    TrinoSqlEngine.startEngine()
    engine = TrinoSqlEngine.currentEngine.get
    connectionUrl = engine.frontendServices.head.connectionUrl
  }

  override def afterAll(): Unit = {
    super.afterAll()
    stopTrinoEngine()
  }

  def stopTrinoEngine(): Unit = {
    if (engine != null) {
      engine.stop()
      engine = null
    }
  }

  protected def getJdbcUrl: String = s"jdbc:hive2://$connectionUrl/$schema;"
}
