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

package org.apache.kyuubi.engine.spark.session

import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.session.USE_CATALOG
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TCloseSessionReq, TOpenSessionReq, TStatusCode}

/**
 * Catalog plugins only read `spark.sql.catalog.<name>.*` once, at initialization. Session configs
 * must therefore be applied before anything in the session open path initializes a catalog,
 * i.e. the session initialization SQLs and `use:catalog`.
 */
class SessionConfBeforeCatalogInitSuite extends WithSparkSQLEngine with HiveJDBCTestHelper {

  private val catalogName = "conf_capture"
  private val tokenKey = s"spark.sql.catalog.$catalogName.token"

  override def withKyuubiConf: Map[String, String] = Map(
    ENGINE_SHARE_LEVEL.key -> "USER",
    s"spark.sql.catalog.$catalogName" -> classOf[ConfCapturingCatalog].getName,
    tokenKey -> "engine-launch")

  override protected def jdbcUrl: String = getJdbcUrl

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    ConfCapturingCatalog.initializedTokens.clear()
  }

  private def openSession(conf: Map[String, String]): Unit = {
    withThriftClient(Some(user)) { client =>
      val req = new TOpenSessionReq()
      req.setUsername(user)
      req.setPassword("anonymous")
      req.setConfiguration(conf.asJava)
      val openResp = client.OpenSession(req)
      val openStatus = openResp.getStatus
      assert(
        openStatus.getStatusCode === TStatusCode.SUCCESS_STATUS,
        String.valueOf(openStatus.getErrorMessage))
      val closeResp = client.CloseSession(new TCloseSessionReq(openResp.getSessionHandle))
      assert(closeResp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
    }
  }

  test("session configs are applied before session initialization SQLs") {
    openSession(Map(
      ENGINE_SESSION_SPARK_INITIALIZE_SQL.key -> s"USE $catalogName",
      tokenKey -> "session-init-sql"))
    assert(ConfCapturingCatalog.initializedTokens.asScala.toSeq === Seq("session-init-sql"))
  }

  test("session configs are applied before use:catalog") {
    openSession(Map(USE_CATALOG -> catalogName, tokenKey -> "use-catalog"))
    assert(ConfCapturingCatalog.initializedTokens.asScala.toSeq === Seq("use-catalog"))
  }
}

object ConfCapturingCatalog {
  val initializedTokens = new ConcurrentLinkedQueue[String]()
}

class ConfCapturingCatalog extends CatalogPlugin {
  private var catalogName: String = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogName = name
    ConfCapturingCatalog.initializedTokens.add(options.get("token"))
  }

  override def name(): String = catalogName
}
