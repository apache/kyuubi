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

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.metrics.MetricsConstants.{OPERATION_STATE, OPERATION_TOTAL}
import org.apache.kyuubi.metrics.MetricsSystem

class KyuubiOperationMetricsSuite extends KyuubiFunSuite {
  test("") {
    val metricsSystem = new MetricsSystem
    metricsSystem.initialize(new KyuubiConf())
    metricsSystem.start()

    val operationTotal = MetricRegistry.name(OPERATION_TOTAL)
    val operationStateInit =
      MetricRegistry.name(OPERATION_STATE, OperationState.INITIALIZED.toString.toLowerCase())
    val operationStateCanceled =
      MetricRegistry.name(OPERATION_STATE, OperationState.CANCELED.toString.toLowerCase())
    print("--- before ---\n")
    print(s"$operationTotal: ${MetricsSystem.counterValue(operationTotal).get}\n")
    print(s"$operationStateInit: ${MetricsSystem.meterValue(operationStateInit).get}\n")
    print(s"$operationStateCanceled: ${MetricsSystem.meterValue(operationStateCanceled).get}\n")
    val operation = new LaunchEngine(null, false)
    operation.setState(OperationState.RUNNING)
    print("--- init ---\n")
    print(s"$operationTotal: ${MetricsSystem.counterValue(operationTotal).get}\n")
    print(s"$operationStateInit: ${MetricsSystem.meterValue(operationStateInit).get}\n")
    print(s"$operationStateCanceled: ${MetricsSystem.meterValue(operationStateCanceled).get}\n")
    operation.setState(OperationState.CANCELED)
    print("--- cancel ---\n")
    print(s"$operationTotal: ${MetricsSystem.counterValue(operationTotal).get}\n")
    print(s"$operationStateInit: ${MetricsSystem.meterValue(operationStateInit).get}\n")
    print(s"$operationStateCanceled: ${MetricsSystem.meterValue(operationStateCanceled).get}\n")
  }
}
