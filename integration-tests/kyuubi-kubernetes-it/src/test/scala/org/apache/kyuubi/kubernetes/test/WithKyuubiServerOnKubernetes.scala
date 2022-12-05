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

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.DefaultKubernetesClient

import org.apache.kyuubi.KyuubiFunSuite

trait WithKyuubiServerOnKubernetes extends KyuubiFunSuite {
  protected def connectionConf: Map[String, String] = Map.empty
  private val miniKubernetesClient: DefaultKubernetesClient = MiniKube.getKubernetesClient

  protected def getJdbcUrl(connectionConf: Map[String, String]): String = {
    val kyuubiServer = getKyuubiPod
    // Kyuubi server state should be running since mvn compile is quite slowly..
    if (!"running".equalsIgnoreCase(kyuubiServer.getStatus.getPhase)) {
      val log =
        miniKubernetesClient
          .pods()
          .withName(kyuubiServer.getMetadata.getName)
          .getLog
      throw new IllegalStateException(
        s"Kyuubi server pod state error: ${kyuubiServer.getStatus.getPhase}, log:\n$log")
    }
    val kyuubiServerIp = getMiniKubeIp
    val kyuubiServerPort =
      kyuubiServer.getSpec.getContainers.get(0).getPorts.get(0).getHostPort
    val connectStr = connectionConf.map(kv => kv._1 + "=" + kv._2).mkString("#", ";", "")
    s"jdbc:hive2://$kyuubiServerIp:$kyuubiServerPort/;$connectStr"
  }

  def getMiniKubeApiMaster: String = miniKubernetesClient.getMasterUrl.toString

  def getKyuubiPod: Pod = miniKubernetesClient.pods().withName("kyuubi-test").get()

  def getKyuubiServerIp: String = getKyuubiPod.getStatus.getPodIP

  def getMiniKubeIp: String = MiniKube.getIp
}
