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

import java.io.{File, FileNotFoundException}

import com.google.common.base.Charsets
import com.google.common.io.Files
import io.fabric8.kubernetes.client.{Config, ConfigBuilder, DefaultKubernetesClient, KubernetesClient, KubernetesClientException}
import io.fabric8.kubernetes.client.okhttp.OkHttpClientFactory
import okhttp3.{Dispatcher, OkHttpClient}

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.KUBERNETES_CONTEXT

object KubernetesUtils extends Logging {

  def buildKubernetesClient(conf: KyuubiConf): KubernetesClient = {
    if (conf.get(KyuubiConf.SERVER_PREFER_BUILD_K8S_CLIENT_FROM_POD_ENV) && Utils.isOnK8s) {
      try {
        buildKubernetesClientFromPodEnv
      } catch {
        case e: KubernetesClientException =>
          error("Fail to build kubernetes client for kubernetes application operation", e)
          null
        case e: FileNotFoundException =>
          error(
            "Fail to build kubernetes client for kubernetes application operation, " +
              "due to file not found",
            e)
          null
      }
    } else {
      val contextOpt = conf.get(KUBERNETES_CONTEXT)
      if (contextOpt.isEmpty) {
        warn("Skip initialize kubernetes client, because of context not set.")
        null
      } else {
        try {
          val client = new DefaultKubernetesClient(Config.autoConfigure(contextOpt.get))
          info(s"Initialized kubernetes client connect to: ${client.getMasterUrl}")
          client
        } catch {
          case e: KubernetesClientException =>
            error("Fail to init kubernetes client for kubernetes application operation", e)
            null
        }
      }
    }
  }

  private def buildKubernetesClientFromPodEnv: KubernetesClient = {
    val nsFile = new File("/var/run/secrets/kubernetes.io/serviceaccount/namespace")

    // use service account for client ca and token
    val ca = new File("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
    val token = new File("/var/run/secrets/kubernetes.io/serviceaccount/token")

    // (tcp://XXX:XXX) => (https://XXX:XXX)
    val master = sys.env("KUBERNETES_PORT").replace("tcp", "https")

    val config = new ConfigBuilder()
      .withMasterUrl(master)
      .withNamespace(Files.asCharSource(nsFile, Charsets.UTF_8).readFirstLine())
      .withOauthToken(Files.asCharSource(token, Charsets.UTF_8).readFirstLine())
      .withCaCertFile(ca.getAbsolutePath)
      .build()

    val dispatcher = new Dispatcher(
      ThreadUtils.newDaemonCachedThreadPool("kubernetes-dispatcher"))
    val factoryWithCustomDispatcher = new OkHttpClientFactory() {
      override protected def additionalConfig(builder: OkHttpClient.Builder): Unit = {
        builder.dispatcher(dispatcher)
      }
    }

    new DefaultKubernetesClient(factoryWithCustomDispatcher.createHttpClient(config), config)
  }
}
