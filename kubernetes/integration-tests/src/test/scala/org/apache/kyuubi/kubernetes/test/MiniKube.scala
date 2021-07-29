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

import io.fabric8.kubernetes.client.{Config, DefaultKubernetesClient}

/**
 * This code copied from Aapache Spark
 * org.apache.spark.deploy.k8s.integrationtest.backend.minikube.Minikube
 */
object MiniKube {
  private val MINIKUBE_STARTUP_TIMEOUT_SECONDS = 60
  private val VERSION_PREFIX = "minikube version: "

  lazy val minikubeVersionString =
    executeMinikube(true, "version").find(_.contains(VERSION_PREFIX)).get

  def executeMinikube(logOutput: Boolean, action: String, args: String*): Seq[String] = {
    ProcessUtils.executeProcess(
      Array("bash", "-c", s"MINIKUBE_IN_STYLE=true minikube $action ${args.mkString(" ")}"),
      MINIKUBE_STARTUP_TIMEOUT_SECONDS, dumpOutput = logOutput).filter { x =>
      !x.contains("There is a newer version of minikube") &&
        !x.contains("https://github.com/kubernetes")
    }
  }

  def getIp: String = {
    executeMinikube(true, "ip").head
  }

  def getKubernetesClient: DefaultKubernetesClient = {
    // only the three-part version number is matched (the optional suffix like "-beta.0" is dropped)
    val versionArrayOpt = "\\d+\\.\\d+\\.\\d+".r
      .findFirstIn(minikubeVersionString.split(VERSION_PREFIX)(1))
      .map(_.split('.').map(_.toInt))

    versionArrayOpt match {
      case Some(Array(x, y, z)) =>
        if (Ordering.Tuple3[Int, Int, Int].lt((x, y, z), (1, 7, 3))) {
          assert(false, s"Unsupported Minikube version is detected: $minikubeVersionString." +
            "For integration testing Minikube version 1.7.3 or greater is expected.")
        }
      case _ =>
        assert(false, s"Unexpected version format detected in `$minikubeVersionString`." +
          "For minikube version a three-part version number is expected (the optional " +
          "non-numeric suffix is intentionally dropped)")
    }

    new DefaultKubernetesClient(Config.autoConfigure("minikube"))
  }
}
