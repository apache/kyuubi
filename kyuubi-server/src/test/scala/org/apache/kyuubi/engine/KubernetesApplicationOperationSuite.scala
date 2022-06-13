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

import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.apache.kyuubi.{KyuubiFunSuite, MiniKube}
import org.apache.kyuubi.config.KyuubiConf

class KubernetesApplicationOperationSuite extends KyuubiFunSuite {
  private val operations = ServiceLoader.load(classOf[ApplicationOperation])
    .asScala.filter(_.getClass.isAssignableFrom(classOf[JpsApplicationOperation]))
  private val kubernetes = operations.head

  test("Init KubernetesApplicationOperation Without conf") {
    kubernetes.initialize(null)
    assert(!kubernetes.isSupported(None))
    assert(!kubernetes.isSupported(Some("k8s")))
  }

  test("Init KubernetesApplicationOperation") {
    val minikube = MiniKube.getKubernetesClient
    val conf = new KyuubiConf()
      .set(KyuubiConf.KUBERNETES_CONTEXT, "minikube")
    kubernetes.initialize(conf)
    assert(kubernetes.isSupported(Some("k8s")))
  }
}
