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

package org.apache.kyuubi.operation.thrift.http

import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.FrontendProtocols
import org.apache.kyuubi.metrics.{MetricsConstants, MetricsSystem}
import org.apache.kyuubi.operation.KyuubiOperationPerUserSuite

class KyuubiOperationThriftHttpPerUserSuite extends KyuubiOperationPerUserSuite {
  override def beforeAll(): Unit = {
    super.beforeAll()
    Thread.sleep(3000)
  }

  override protected val frontendProtocols: Seq[KyuubiConf.FrontendProtocols.Value] =
    FrontendProtocols.THRIFT_HTTP :: Nil

  override protected def getJdbcUrl: String =
    s"jdbc:hive2://${server.frontendServices.head.connectionUrl}/;transportMode=http;" +
      s"httpPath=cliservice;"

  override protected lazy val httpMode = true

  test("thrift http connection metric") {
    val totalConnections =
      MetricsSystem.counterValue(MetricsConstants.THRIFT_HTTP_CONN_TOTAL).getOrElse(0L)
    val failedConnections =
      MetricsSystem.counterValue(MetricsConstants.THRIFT_HTTP_CONN_FAIL).getOrElse(0L)

    withJdbcStatement() { _ =>
      assert(MetricsSystem.counterValue(
        MetricsConstants.THRIFT_HTTP_CONN_TOTAL).getOrElse(0L) - totalConnections === 1)
      assert(MetricsSystem.counterValue(
        MetricsConstants.THRIFT_HTTP_CONN_OPEN).getOrElse(0L) === 1)
      assert(MetricsSystem.counterValue(
        MetricsConstants.THRIFT_HTTP_CONN_FAIL).getOrElse(0L) - failedConnections === 0)

      withJdbcStatement() { _ =>
        assert(MetricsSystem.counterValue(
          MetricsConstants.THRIFT_HTTP_CONN_TOTAL).getOrElse(0L) - totalConnections === 2)
        assert(MetricsSystem.counterValue(
          MetricsConstants.THRIFT_HTTP_CONN_OPEN).getOrElse(0L) === 2)
        assert(MetricsSystem.counterValue(
          MetricsConstants.THRIFT_HTTP_CONN_FAIL).getOrElse(0L) - failedConnections === 0)
      }

    }

    eventually(timeout(10.seconds), interval(200.milliseconds)) {
      assert(MetricsSystem.counterValue(
        MetricsConstants.THRIFT_HTTP_CONN_TOTAL).getOrElse(0L) - totalConnections === 2)
      assert(MetricsSystem.counterValue(
        MetricsConstants.THRIFT_HTTP_CONN_OPEN).getOrElse(0L) === 0)
      assert(MetricsSystem.counterValue(
        MetricsConstants.THRIFT_HTTP_CONN_FAIL).getOrElse(0L) - failedConnections === 0)
    }
  }
}
