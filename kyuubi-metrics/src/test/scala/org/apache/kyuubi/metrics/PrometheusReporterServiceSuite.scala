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

package org.apache.kyuubi.metrics

import org.apache.kyuubi.KyuubiFunSuite

class PrometheusReporterServiceSuite extends KyuubiFunSuite {
  test("apply extra labels on metrics") {
    val labels = Map("instance" -> "kyuubi-0")
    val labelStr = labels.map { case (k, v) => s"""$k="$v"""" }.toArray.sorted.mkString(",")
    val metrics1 = "kyuubi_backend_service_close_operation{quantile=\"0.5\",}"
    assert(PrometheusReporterService.applyExtraLabels(metrics1, labelStr) ==
      "kyuubi_backend_service_close_operation{quantile=\"0.5\",instance=\"kyuubi-0\"}")
    val metrics2 = "kyuubi_backend_service_close_operation"
    assert(PrometheusReporterService.applyExtraLabels(metrics2, labelStr) ==
      "kyuubi_backend_service_close_operation{instance=\"kyuubi-0\"}")
  }
}
