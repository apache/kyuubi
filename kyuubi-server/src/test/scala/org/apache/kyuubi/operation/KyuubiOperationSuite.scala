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

package org.apache.kyuubi.operation

import com.codahale.metrics.MetricRegistry
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.kyuubi.{KyuubiFunSuite, KyuubiSQLException}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.metrics.{MetricsConf, MetricsConstants, MetricsSystem}
import org.apache.kyuubi.operation.OperationState.OperationState
import org.apache.kyuubi.session.{Session, SessionHandle, SessionManager}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10

class KyuubiOperationSuite extends KyuubiFunSuite {

  private var metricsSystem: MetricsSystem = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    metricsSystem = new MetricsSystem()
    metricsSystem.initialize(KyuubiConf()
      .set(MetricsConf.METRICS_REPORTERS, Set.empty[String]))
    metricsSystem.start()
  }

  override def afterEach(): Unit = {
    try {
      if (metricsSystem != null) {
        metricsSystem.stop()
        metricsSystem = null
      }
    } finally {
      super.afterEach()
    }
  }

  test("do not update operation state metrics on stale terminal state transition") {
    val operation = new TestKyuubiOperation(mockSession())

    operation.transitState(OperationState.RUNNING)
    operation.transitState(OperationState.FINISHED)

    val canceledMetric = stateMetric(OperationState.CANCELED)
    val runningMetric = stateMetric(OperationState.RUNNING)
    val canceledCount = MetricsSystem.meterValue(canceledMetric).getOrElse(0L)
    val runningCount = MetricsSystem.meterValue(runningMetric).getOrElse(0L)

    intercept[KyuubiSQLException] {
      operation.transitState(OperationState.CANCELED)
    }

    assert(MetricsSystem.meterValue(canceledMetric).getOrElse(0L) === canceledCount)
    assert(MetricsSystem.meterValue(runningMetric).getOrElse(0L) === runningCount)
    assert(operation.getStatus.state === OperationState.FINISHED)
  }

  private def mockSession(): Session = {
    val conf = KyuubiConf()
    val sessionManager = mock[SessionManager]
    when(sessionManager.getConf).thenReturn(conf)

    val session = mock[Session]
    when(session.protocol).thenReturn(HIVE_CLI_SERVICE_PROTOCOL_V10)
    when(session.handle).thenReturn(SessionHandle())
    when(session.user).thenReturn("kyuubi")
    when(session.sessionManager).thenReturn(sessionManager)
    session
  }

  private def stateMetric(state: OperationState): String = {
    MetricRegistry.name(
      MetricsConstants.OPERATION_STATE,
      classOf[TestKyuubiOperation].getSimpleName,
      state.toString.toLowerCase)
  }
}

private class TestKyuubiOperation(session: Session) extends KyuubiOperation(session) {

  def transitState(newState: OperationState): Unit = setState(newState)

  override protected def runInternal(): Unit = {}
}
