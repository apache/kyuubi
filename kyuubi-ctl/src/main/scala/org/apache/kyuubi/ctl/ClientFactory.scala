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

import java.util.HashMap

import scala.reflect.runtime.{universe => ru}
import scala.reflect.runtime.universe._

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.client.KyuubiRestClient
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ctl.CtlConf._

object ClientFactory {

  private[ctl] def getKyuubiRestClient(
      cliArgs: CliConfig,
      map: HashMap[String, Object],
      conf: KyuubiConf): KyuubiRestClient = {
    val version = getApiVersion(map)
    val hostUrl = getRestConfig("hostUrl", conf.get(CTL_REST_CLIENT_BASE_URL).get, cliArgs, map)
    val authSchema = getRestConfig("authSchema", conf.get(CTL_REST_CLIENT_AUTH_SCHEMA),
      cliArgs, map)

    var kyuubiRestClient: KyuubiRestClient = null
    authSchema match {
      case "basic" =>
        val username = getRestConfig("username", null, cliArgs, map)
        val password = cliArgs.commonOpts.password
        kyuubiRestClient = KyuubiRestClient.builder(hostUrl)
          .apiVersion(KyuubiRestClient.ApiVersion.valueOf(version))
          .authSchema(KyuubiRestClient.AuthSchema.BASIC)
          .username(username)
          .password(password)
          .build()
      case "spnego" =>
        val spnegoHost =
          getRestConfig("spnegoHost", conf.get(CTL_REST_CLIENT_SPNEGO_HOST).get, cliArgs, map)
        kyuubiRestClient = KyuubiRestClient.builder(hostUrl)
          .apiVersion(KyuubiRestClient.ApiVersion.valueOf(version))
          .authSchema(KyuubiRestClient.AuthSchema.SPNEGO)
          .spnegoHost(spnegoHost)
          .build()
      case _ => throw new KyuubiException(s"Unsupported auth schema: $authSchema")
    }
    kyuubiRestClient
  }

  private def getApiVersion(map: HashMap[String, Object]): String = {
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
      cliArgs: CliConfig,
      map: HashMap[String, Object]): String = {
    var configValue: String = null
    // get value from command line
    val commonOpts = cliArgs.commonOpts
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val instanceMirror = mirror.reflect(commonOpts)
    val field = typeOf[CommonOpts].decl(TermName(key)).asTerm.accessed.asTerm
    val fieldMirror = instanceMirror.reflectField(field)
    configValue = fieldMirror.get.asInstanceOf[String]

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
