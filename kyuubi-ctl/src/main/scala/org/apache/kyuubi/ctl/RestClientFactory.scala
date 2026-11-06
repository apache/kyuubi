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
package org.apache.kyuubi.ctl

import java.util.{Map => JMap}

import scala.collection.JavaConverters._

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.client.KyuubiRestClient
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ctl.CtlConf._
import org.apache.kyuubi.ctl.opt.CliConfig

object RestClientFactory {

  private[ctl] def withKyuubiRestClient[T](
      cliConfig: CliConfig,
      map: JMap[String, Object],
      conf: KyuubiConf)(f: KyuubiRestClient => T): T = {
    val kyuubiRestClient: KyuubiRestClient =
      RestClientFactory.getKyuubiRestClient(cliConfig, map, conf)
    try {
      f(kyuubiRestClient)
    } finally {
      kyuubiRestClient.close()
    }
  }

  private[ctl] def withKyuubiInstanceRestClient(
      kyuubiRestClient: KyuubiRestClient,
      kyuubiInstance: String)(f: KyuubiRestClient => Unit): Unit = {
    val kyuubiInstanceRestClient = kyuubiRestClient.clone()
    val hostUrls = Option(kyuubiInstance).map(instance => s"http://$instance").toSeq ++
      kyuubiRestClient.getHostUrls.asScala
    kyuubiInstanceRestClient.setHostUrls(hostUrls.asJava)
    try {
      f(kyuubiInstanceRestClient)
    } finally {
      kyuubiInstanceRestClient.close()
    }
  }

  private def getKyuubiRestClient(
      cliConfig: CliConfig,
      map: JMap[String, Object],
      conf: KyuubiConf): KyuubiRestClient = {
    val version = getApiVersion(map)
    val hostUrl =
      getRestConfig("hostUrl", conf.get(CTL_REST_CLIENT_BASE_URL).orNull, cliConfig, map)
    val authSchema =
      getRestConfig("authSchema", conf.get(CTL_REST_CLIENT_AUTH_SCHEMA), cliConfig, map)

    val maxAttempts = conf.get(CTL_REST_CLIENT_REQUEST_MAX_ATTEMPTS)
    val attemptWaitTime = conf.get(CTL_REST_CLIENT_REQUEST_ATTEMPT_WAIT).toInt
    val connectionTimeout = conf.get(CTL_REST_CLIENT_CONNECT_TIMEOUT).toInt
    val socketTimeout = conf.get(CTL_REST_CLIENT_SOCKET_TIMEOUT).toInt

    var kyuubiRestClient: KyuubiRestClient = null
    authSchema.toLowerCase match {
      case "basic" =>
        val username = getRestConfig("username", null, cliConfig, map)
        val password = cliConfig.commonOpts.password
        kyuubiRestClient = KyuubiRestClient.builder(hostUrl)
          .apiVersion(KyuubiRestClient.ApiVersion.valueOf(version))
          .authHeaderMethod(KyuubiRestClient.AuthHeaderMethod.BASIC)
          .username(username)
          .password(password)
          .connectionTimeout(connectionTimeout)
          .socketTimeout(socketTimeout)
          .maxAttempts(maxAttempts)
          .attemptWaitTime(attemptWaitTime)
          .build()
      case "spnego" =>
        val spnegoHost =
          getRestConfig("spnegoHost", conf.get(CTL_REST_CLIENT_SPNEGO_HOST).orNull, cliConfig, map)
        kyuubiRestClient = KyuubiRestClient.builder(hostUrl)
          .apiVersion(KyuubiRestClient.ApiVersion.valueOf(version))
          .authHeaderMethod(KyuubiRestClient.AuthHeaderMethod.SPNEGO)
          .spnegoHost(spnegoHost)
          .connectionTimeout(connectionTimeout)
          .socketTimeout(socketTimeout)
          .maxAttempts(maxAttempts)
          .attemptWaitTime(attemptWaitTime)
          .build()
      case _ => throw new KyuubiException(s"Unsupported auth schema: $authSchema")
    }
    kyuubiRestClient
  }

  private def getApiVersion(map: JMap[String, Object]): String = {
    var version: String = "V1"
    if (map != null) {
      val configuredVersion = map.get("apiVersion").asInstanceOf[String].toUpperCase
      if (StringUtils.isNotBlank(configuredVersion)) {
        version = configuredVersion
      }
    }
    version
  }

  private def getRestConfig(
      key: String,
      defaultValue: String,
      cliConfig: CliConfig,
      map: JMap[String, Object]): String = {
    // get value from command line
    val commonOpts = cliConfig.commonOpts
    var configValue: String = key match {
      case "hostUrl" => commonOpts.hostUrl
      case "authSchema" => commonOpts.authSchema
      case "username" => commonOpts.username
      case "spnegoHost" => commonOpts.spnegoHost
      case _ => null
    }

    // get value from map
    if (StringUtils.isBlank(configValue) && map != null) {
      configValue = map.get(key).asInstanceOf[String]
    }

    // get value from default
    if (StringUtils.isBlank(configValue)) {
      configValue = defaultValue
    }

    configValue
  }

}
