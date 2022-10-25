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

package org.apache.kyuubi.kubernetes.test

import io.fabric8.kubernetes.client.Config

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{KUBERNETES_CONTEXT, KUBERNETES_MASTER}
import org.apache.kyuubi.util.KubernetesUtils

class KubernetesUtilsTest extends KyuubiFunSuite {

  test("Test kubernetesUtils build Kubernetes client") {
    val testMaster = "https://localhost:12345/"
    withSystemProperty(Map(Config.KUBERNETES_MASTER_SYSTEM_PROPERTY -> testMaster)) {
      val conf = KyuubiConf()
      val client1 = KubernetesUtils.buildKubernetesClient(conf)
      assert(client1.nonEmpty && client1.get.getMasterUrl.toString.equals(testMaster))

      // start up minikube
      MiniKube.getIp
      conf.set(KUBERNETES_CONTEXT.key, "minikube")
      val client2 = KubernetesUtils.buildKubernetesClient(conf)
      assert(client2.nonEmpty && client2.get.getMasterUrl.equals(
        MiniKube.getKubernetesClient.getMasterUrl))

      // user set master uri should replace uri in context
      val master = "https://kyuubi-test:8443/"
      conf.set(KUBERNETES_MASTER.key, master)
      val client3 = KubernetesUtils.buildKubernetesClient(conf)
      assert(client3.nonEmpty && client3.get.getMasterUrl.toString.equals(master))
    }
  }
}
