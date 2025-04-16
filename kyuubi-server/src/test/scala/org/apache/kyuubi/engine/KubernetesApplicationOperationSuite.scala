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

package org.apache.kyuubi.engine

import org.apache.kyuubi.{KyuubiException, KyuubiFunSuite}
import org.apache.kyuubi.config.KyuubiConf

class KubernetesApplicationOperationSuite extends KyuubiFunSuite {

  test("test check kubernetes info") {
    val kyuubiConf = KyuubiConf()
    kyuubiConf.set(KyuubiConf.KUBERNETES_CONTEXT_ALLOW_LIST.key, "1,2")
    kyuubiConf.set(KyuubiConf.KUBERNETES_NAMESPACE_ALLOW_LIST.key, "ns1,ns2")

    val operation = new KubernetesApplicationOperation()
    operation.initialize(kyuubiConf)

    operation.checkKubernetesInfo(KubernetesInfo(None, None))
    operation.checkKubernetesInfo(KubernetesInfo(Some("1"), None))
    operation.checkKubernetesInfo(KubernetesInfo(Some("2"), None))
    operation.checkKubernetesInfo(KubernetesInfo(Some("1"), Some("ns1")))
    operation.checkKubernetesInfo(KubernetesInfo(Some("1"), Some("ns2")))
    operation.checkKubernetesInfo(KubernetesInfo(Some("2"), Some("ns1")))
    operation.checkKubernetesInfo(KubernetesInfo(Some("2"), Some("ns2")))

    intercept[KyuubiException] {
      operation.checkKubernetesInfo(KubernetesInfo(Some("3"), Some("ns1")))
    }
    intercept[KyuubiException] {
      operation.checkKubernetesInfo(KubernetesInfo(Some("1"), Some("ns3")))
    }
    intercept[KyuubiException] {
      operation.checkKubernetesInfo(KubernetesInfo(Some("3"), None))
    }
    intercept[KyuubiException] {
      operation.checkKubernetesInfo(KubernetesInfo(None, Some("ns3")))
    }

    kyuubiConf.unset(KyuubiConf.KUBERNETES_CONTEXT_ALLOW_LIST.key)
    operation.checkKubernetesInfo(KubernetesInfo(Some("3"), None))
    kyuubiConf.unset(KyuubiConf.KUBERNETES_NAMESPACE_ALLOW_LIST.key)
    operation.checkKubernetesInfo(KubernetesInfo(None, Some("ns3")))
  }

  test("build spark app url") {
    val sparkAppUrlPattern1 = "http://{{SPARK_APP_ID}}.ingress.balabala"
    val sparkAppUrlPattern2 =
      "http://{{SPARK_DRIVER_SVC}}.{{KUBERNETES_NAMESPACE}}.svc:{{SPARK_UI_PORT}}"
    val sparkAppUrlPattern3 =
      "http://{{SPARK_DRIVER_SVC}}.{{KUBERNETES_NAMESPACE}}.svc" +
        ".{{KUBERNETES_CONTEXT}}.k8s.io:{{SPARK_UI_PORT}}"

    val sparkAppId = "spark-123"
    val sparkDriverSvc = "spark-456-driver-svc"
    val kubernetesContext = "1"
    val kubernetesNamespace = "kyuubi"
    val sparkUiPort = 4040

    assert(KubernetesApplicationOperation.buildSparkAppUrl(
      sparkAppUrlPattern1,
      sparkAppId,
      sparkDriverSvc,
      kubernetesContext,
      kubernetesNamespace,
      sparkUiPort) === s"http://$sparkAppId.ingress.balabala")

    assert(KubernetesApplicationOperation.buildSparkAppUrl(
      sparkAppUrlPattern2,
      sparkAppId,
      sparkDriverSvc,
      kubernetesContext,
      kubernetesNamespace,
      sparkUiPort) === s"http://$sparkDriverSvc.$kubernetesNamespace.svc:$sparkUiPort")

    assert(KubernetesApplicationOperation.buildSparkAppUrl(
      sparkAppUrlPattern3,
      sparkAppId,
      sparkDriverSvc,
      kubernetesContext,
      kubernetesNamespace,
      sparkUiPort) ===
      s"http://$sparkDriverSvc.$kubernetesNamespace.svc.$kubernetesContext.k8s.io:$sparkUiPort")
  }

  test("get kubernetes client initialization info") {
    val kyuubiConf = KyuubiConf()
    kyuubiConf.set(
      KyuubiConf.KUBERNETES_CLIENT_INITIALIZE_LIST.key,
      "c1:ns1,c1:ns2,c2:ns1,c2:ns2,c1:,:ns1")

    val operation = new KubernetesApplicationOperation()
    assert(operation.getKubernetesClientInitializeInfo(kyuubiConf) ===
      Array(
        KubernetesInfo(Some("c1"), Some("ns1")),
        KubernetesInfo(Some("c1"), Some("ns2")),
        KubernetesInfo(Some("c2"), Some("ns1")),
        KubernetesInfo(Some("c2"), Some("ns2")),
        KubernetesInfo(Some("c1"), None),
        KubernetesInfo(None, Some("ns1"))))
  }
}
