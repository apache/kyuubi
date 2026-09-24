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

package org.apache.kyuubi.engine.spark.connect

import scala.util.Try
import scala.util.control.NonFatal

import org.apache.spark.SparkConf

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_SHARE_LEVEL, ENGINE_SPARK_CONNECT_ENABLED}
import org.apache.kyuubi.engine.ShareLevel
import org.apache.kyuubi.util.reflect.{DynFields, DynMethods, ReflectUtils}

/**
 * Starts Spark Connect inside the Spark SQL engine, next to the Thrift frontend, by writing the
 * Spark configs that make the driver load the Connect plugin.
 *
 * Spark Connect classes are never referenced at compile time: `io.grpc` is relocated to
 * `org.apache.kyuubi.shade.io.grpc` in this module, and the Scala signatures of
 * `SparkConnectService` carry `io.grpc` types, so a direct reference does not compile. That also
 * keeps the engine building against the default `spark-3.5` profile, where the feature stays off.
 */
object SparkConnectLauncher extends Logging {

  final val PLUGINS_KEY = "spark.plugins"
  final val BINDING_PORT_KEY = "spark.connect.grpc.binding.port"
  final val AUTHENTICATE_TOKEN_KEY = "spark.connect.authenticate.token"
  final val AUTHENTICATE_TOKEN_ENV = "SPARK_CONNECT_AUTHENTICATE_TOKEN"
  final val DRIVER_HOST_KEY = "spark.driver.host"

  final private val CONNECT_PLUGIN_CLASS = "org.apache.spark.sql.connect.SparkConnectPlugin"
  final private val CONNECT_CONFIG_CLASS = "org.apache.spark.sql.connect.config.Connect$"
  final private val CONNECT_SERVICE_CLASS =
    "org.apache.spark.sql.connect.service.SparkConnectService$"

  def isEnabled(kyuubiConf: KyuubiConf): Boolean = kyuubiConf.get(ENGINE_SPARK_CONNECT_ENABLED)

  /**
   * Only the USER share level is supported for now: a Spark Connect client authenticates with a
   * token that is shared by everyone reaching the engine, and the plan runs as the engine's own
   * user, so a shared engine would execute one user's plans under another user's identity.
   *
   * `SparkProcessBuilder` decides whether an engine serves Connect at all, where the share level
   * and doAs are the server's own values; this is the engine refusing a combination that should
   * never have been launched.
   */
  def checkShareLevel(kyuubiConf: KyuubiConf): Unit = {
    val shareLevel = kyuubiConf.get(ENGINE_SHARE_LEVEL)
    if (shareLevel != ShareLevel.USER.toString) {
      throw new KyuubiException(
        s"${ENGINE_SPARK_CONNECT_ENABLED.key} requires ${ENGINE_SHARE_LEVEL.key}=" +
          s"${ShareLevel.USER}, but it is $shareLevel. Other share levels are not supported yet.")
    }
  }

  /**
   * Spark 3.5 ships Spark Connect without any authentication - `spark.connect.authenticate.token`
   * arrived in 4.0 - so enabling it there would open an unauthenticated endpoint on the engine.
   * The capability is probed instead of comparing versions.
   */
  def checkRuntimeSupport(
      pluginClass: String = CONNECT_PLUGIN_CLASS,
      configClass: String = CONNECT_CONFIG_CLASS): Unit = {
    if (!ReflectUtils.isClassLoadable(pluginClass)) {
      throw new KyuubiException(
        s"${ENGINE_SPARK_CONNECT_ENABLED.key} is set, but $pluginClass is not on the" +
          " engine classpath. Spark Connect ships in the spark-connect jar of a Spark 4.0 or" +
          " later distribution.")
    }
    if (!authenticationSupported(configClass)) {
      throw new KyuubiException(
        s"${ENGINE_SPARK_CONNECT_ENABLED.key} is set, but this Spark runtime has a Spark Connect" +
          s" without authentication support - $AUTHENTICATE_TOKEN_KEY was introduced in Spark" +
          " 4.0. Enabling it here would expose an unauthenticated endpoint.")
    }
  }

  /**
   * The engine refuses to open the endpoint without a pre-shared token. Generating one at engine
   * launch is a separate change; today the deployment supplies it.
   *
   * The config wins over the environment even when it is empty, because that is the order
   * `Connect.getAuthenticateToken` reads them in: an empty config would install an interceptor
   * with an empty key rather than fall back to the environment.
   */
  def checkAuthenticateToken(sparkConf: SparkConf): Unit = {
    val token = sparkConf.getOption(AUTHENTICATE_TOKEN_KEY)
      .orElse(sys.env.get(AUTHENTICATE_TOKEN_ENV))
    if (!token.exists(_.nonEmpty)) {
      throw new KyuubiException(
        s"${ENGINE_SPARK_CONNECT_ENABLED.key} is set, but no Spark Connect token is configured." +
          s" Set $AUTHENTICATE_TOKEN_KEY or the $AUTHENTICATE_TOKEN_ENV environment variable so" +
          " that clients are authenticated.")
    }
  }

  /**
   * The Spark configs to add so that the driver starts Connect. Configs the deployment set are
   * kept as they are.
   */
  def sparkConfOverrides(sparkConf: SparkConf): Seq[(String, String)] = {
    val plugins = sparkConf.get(PLUGINS_KEY, "").split(",").map(_.trim).filter(_.nonEmpty)
    val pluginOverride =
      if (plugins.contains(CONNECT_PLUGIN_CLASS)) Nil
      else Seq(PLUGINS_KEY -> (plugins :+ CONNECT_PLUGIN_CLASS).mkString(","))
    // Kyuubi runs one engine per user on a host, so bind an ephemeral port unless the deployment
    // picked one; the port that was actually bound is read back afterwards.
    val portOverride =
      if (sparkConf.contains(BINDING_PORT_KEY)) Nil else Seq(BINDING_PORT_KEY -> "0")
    pluginOverride ++ portOverride
  }

  def setup(kyuubiConf: KyuubiConf, sparkConf: SparkConf): Unit = {
    checkShareLevel(kyuubiConf)
    checkRuntimeSupport()
    checkAuthenticateToken(sparkConf)
    sparkConfOverrides(sparkConf).foreach { case (key, value) => sparkConf.set(key, value) }
  }

  /**
   * The endpoint a Spark Connect client reaches, once the plugin has started the service.
   *
   * The host comes from `spark.driver.host` rather than `SparkConnectService.hostAddress`: the
   * latter is the local canonical host name, which on Kubernetes is the pod name and does not
   * resolve outside the pod.
   */
  def boundEndpoint(
      sparkConf: SparkConf,
      serviceClass: String = CONNECT_SERVICE_CLASS): Option[(String, Int)] = {
    try {
      val service = DynFields.builder()
        .impl(serviceClass, "MODULE$")
        .buildChecked[Object]()
        .get(null)
      val port = ReflectUtils.invokeAs[Int](service, "localPort")
      sparkConf.getOption(DRIVER_HOST_KEY).map(_ -> port)
    } catch {
      case NonFatal(e) =>
        warn("Failed to read the Spark Connect endpoint back from the driver", e)
        None
    }
  }

  private[connect] def authenticationSupported(configClass: String): Boolean =
    Try(DynMethods.builder("getAuthenticateToken").impl(configClass).buildChecked()).isSuccess
}
