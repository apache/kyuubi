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

  // Reproduces the scenario from a real-world bug report: a moderately long but not
  // extreme `spark.app.name` (well within the 253-char pod name budget) still produces
  // a driver pod name over 63 characters. That name is later reused as the value of a
  // Kubernetes *label* (e.g. when Spark or Kyuubi tag executor pods with the driver pod
  // name), and label values are restricted to the DNS label limit of 63 characters,
  // independent of the 253-char DNS subdomain limit that applies to pod *names*.
  test("driver pod name should also fall back when it would exceed " +
    "the 63-char K8s label length limit, even if it satisfies the 253-char pod name limit") {
    val moderateAppName =
      "kyuubi_USER_SPARK_SQL_someuser_default_73bce6a4-df00-403e-bc5d-d1721e515f9d"
    val refId = "73bce6a4-df00-403e-bc5d-d1721e515f9d"
    val shortNamespace = "default"

    val podName =
      KubernetesUtils.generateDriverPodName(moderateAppName, refId, shortNamespace, false)

    // The resolved name must fall back to the short, safe form because the "preserve the
    // app name" branch would exceed the 63-char K8s DNS label limit (see
    // https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/
    // #syntax-and-character-set), even though it is nowhere near the 253-char pod name /
    // kubelet log path budget.
    assert(podName === s"kyuubi-$refId-driver")
    assert(podName.length <= KubernetesUtils.DRIVER_POD_NAME_AS_LABEL_MAX_LENGTH)
    assert(podLogsDirectoryNameLength(shortNamespace, podName) <=
      KubernetesUtils.DRIVER_POD_NAME_MAX_LENGTH)
  }

  private def podLogsDirectoryNameLength(namespace: String, podName: String): Int = {
    s"${namespace}_${podName}_$podUid".length
  }
}
