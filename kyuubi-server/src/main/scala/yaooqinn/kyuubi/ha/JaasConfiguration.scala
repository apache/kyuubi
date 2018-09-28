/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.ha

import javax.security.auth.login.{AppConfigurationEntry, Configuration}
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag

import scala.collection.JavaConverters._

import org.apache.hadoop.security.authentication.util.KerberosUtil

private[ha] class JaasConfiguration(
    loginContextName: String,
    principal: String,
    keyTabFile: String) extends Configuration {

  final private[this] val baseConfig: Configuration = Configuration.getConfiguration

  override def getAppConfigurationEntry(appName: String): Array[AppConfigurationEntry] = {
    appName match {
      case `loginContextName` =>
        val krbOptions = Map(
          "doNotPrompt" -> "true",
          "storeKey" -> "true",
          "useKeyTab" -> "true",
          "principal" -> this.principal,
          "keyTab" -> this.keyTabFile,
          "refreshKrb5Config" -> "true").asJava
        val entry = new AppConfigurationEntry(
          KerberosUtil.getKrb5LoginModuleName,
          LoginModuleControlFlag.REQUIRED,
          krbOptions)
        Array(entry)
      case _ => baseConfig.getAppConfigurationEntry(appName)
    }
  }
}
