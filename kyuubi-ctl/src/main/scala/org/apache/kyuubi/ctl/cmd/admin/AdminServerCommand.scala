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
package org.apache.kyuubi.ctl.cmd.admin

import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.client.AdminRestApi
import org.apache.kyuubi.ctl.CliConfig
import org.apache.kyuubi.ctl.RestClientFactory.withKyuubiRestClient
import org.apache.kyuubi.ctl.cmd.Command
import org.apache.kyuubi.ctl.util.Validator

class AdminServerCommand(cliConfig: CliConfig) extends Command[String](cliConfig) {
  override def validate(): Unit = {
    Validator.validateAdminCommand(normalizedCliConfig)
  }

  override def doRun(): String = {
    withKyuubiRestClient(normalizedCliConfig, null, conf) { kyuubiRestClient =>
      val adminRestApi = new AdminRestApi(kyuubiRestClient)
      normalizedCliConfig.administerOpts.cmd match {
        case "refreshHadoopConf" => adminRestApi.refreshHadoopConf()
        case cmd => throw new KyuubiException(s"Invalid administer command:$cmd")
      }
    }
  }

  override def render(resp: String): Unit = {
    info(resp)
  }
}
