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

package org.apache.kyuubi.util

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.spark.SparkProcessBuilder
import org.apache.kyuubi.engine.spark.SparkProcessBuilder._

class KubernetesPodNameSuite extends KyuubiFunSuite {

  private val namespace = "n" * 63
  private val engineRefId = "kyuubi-test-engine"
  private val longAppName = "a" * 160
  private val podUid = "u" * 36

  test("driver pod name should reserve kubelet pod logs directory budget") {
    val podName =
      KubernetesUtils.generateDriverPodName(longAppName, engineRefId, namespace, false)

    assert(podName === s"kyuubi-$engineRefId-driver")
    assert(podLogsDirectoryNameLength(namespace, podName) <=
      KubernetesUtils.DRIVER_POD_NAME_MAX_LENGTH)
  }

  test("executor pod name prefix should reserve kubelet pod logs directory budget") {
    val prefix =
      KubernetesUtils.generateExecutorPodNamePrefix(longAppName, engineRefId, namespace, false)
    val podName = s"$prefix-exec-${Int.MaxValue}"

    assert(prefix === s"kyuubi-$engineRefId")
    assert(podLogsDirectoryNameLength(namespace, podName) <=
      KubernetesUtils.DRIVER_POD_NAME_MAX_LENGTH)
  }

  test("SparkProcessBuilder should use spark kubernetes namespace for pod name budget") {
    val builder = new SparkProcessBuilder(
      "kyuubi",
      true,
      KyuubiConf().set(MASTER_KEY, "k8s://internal").set(DEPLOY_MODE_KEY, "cluster"),
      engineRefId)
    val conf = Map(APP_KEY -> longAppName, KUBERNETES_NAMESPACE_KEY -> namespace)
    val podNameConf = builder.appendPodNameConf(conf)

    assert(podNameConf(KUBERNETES_DRIVER_POD_NAME) === s"kyuubi-$engineRefId-driver")
    assert(podNameConf(KUBERNETES_EXECUTOR_POD_NAME_PREFIX) === s"kyuubi-$engineRefId")
  }

  private def podLogsDirectoryNameLength(namespace: String, podName: String): Int = {
    s"${namespace}_${podName}_$podUid".length
  }
}
