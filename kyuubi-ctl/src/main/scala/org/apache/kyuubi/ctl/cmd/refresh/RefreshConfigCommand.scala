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

package org.apache.kyuubi.ctl.cmd.refresh

import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.client.AdminRestApi
import org.apache.kyuubi.ctl.RestClientFactory.withKyuubiRestClient
import org.apache.kyuubi.ctl.cmd.AdminCtlCommand
import org.apache.kyuubi.ctl.cmd.refresh.RefreshConfigCommandConfigType.{DENY_IPS, DENY_USERS, HADOOP_CONF, KUBERNETES_CONF, UNLIMITED_USERS, USER_DEFAULTS_CONF}
import org.apache.kyuubi.ctl.opt.CliConfig
import org.apache.kyuubi.ctl.util.{Tabulator, Validator}

class RefreshConfigCommand(cliConfig: CliConfig) extends AdminCtlCommand[String](cliConfig) {
  def validate(): Unit = {
    Validator.validateAdminConfigType(cliConfig)
  }

  def doRun(): String = {
    withKyuubiRestClient(normalizedCliConfig, null, conf) { kyuubiRestClient =>
      val adminRestApi = new AdminRestApi(kyuubiRestClient)
      normalizedCliConfig.adminConfigOpts.configType match {
        case HADOOP_CONF => adminRestApi.refreshHadoopConf()
        case USER_DEFAULTS_CONF => adminRestApi.refreshUserDefaultsConf()
        case KUBERNETES_CONF => adminRestApi.refreshKubernetesConf()
        case UNLIMITED_USERS => adminRestApi.refreshUnlimitedUsers()
        case DENY_USERS => adminRestApi.refreshDenyUsers()
        case DENY_IPS => adminRestApi.refreshDenyIps()
        case configType => throw new KyuubiException(s"Invalid config type:$configType")
      }
    }
  }

  def render(resp: String): Unit = {
    info(Tabulator.format("", Array("Response"), Array(Array(resp))))
  }
}
object RefreshConfigCommandConfigType {
  final val HADOOP_CONF = "hadoopConf"
  final val USER_DEFAULTS_CONF = "userDefaultsConf"
  final val KUBERNETES_CONF = "kubernetesConf"
  final val UNLIMITED_USERS = "unlimitedUsers"
  final val DENY_USERS = "denyUsers"
  final val DENY_IPS = "denyIps"
}
