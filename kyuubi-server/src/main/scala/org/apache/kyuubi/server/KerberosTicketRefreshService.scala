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

package org.apache.kyuubi.server

import java.util.concurrent.TimeUnit

import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.util.{KyuubiHadoopUtils, ThreadUtils}

class KerberosTicketRefreshService() extends AbstractService("KerberosTicketRefreshService") {

  private val executor = ThreadUtils.newDaemonSingleThreadScheduledExecutor(getName)

  private var refreshInterval: Long = _
  private var keytabLoginMaxAttempts: Int = _
  @volatile private var keytabLoginAttempts: Int = _
  private var tgtRenewalTask: Runnable = _

  override def initialize(conf: KyuubiConf): Unit = {
    if (UserGroupInformation.isSecurityEnabled) {
      val keytab = conf.get(KyuubiConf.SERVER_KEYTAB)
      val principal = conf.get(KyuubiConf.SERVER_PRINCIPAL)
        .map(KyuubiHadoopUtils.getServerPrincipal)
      refreshInterval = conf.get(KyuubiConf.KINIT_INTERVAL)
      keytabLoginMaxAttempts = conf.get(KyuubiConf.KINIT_MAX_ATTEMPTS)

      require(keytab.nonEmpty && principal.nonEmpty, "principal or keytab is missing")
      UserGroupInformation.loginUserFromKeytab(principal.get, keytab.get)

      tgtRenewalTask = new Runnable {
        override def run(): Unit = {
          try {
            UserGroupInformation.getCurrentUser.reloginFromKeytab()
          } catch {
            case e: Exception =>
              if (keytabLoginAttempts >= keytabLoginMaxAttempts) {
                error(s"Failed to refresh ticket with $keytabLoginAttempts attempts, will exit...")
                System.exit(-1)
              }
              keytabLoginAttempts += 1
              error(s"Failed to login from  $keytab with principal[$principal] for" +
                s" ($keytabLoginAttempts/$keytabLoginMaxAttempts) times", e)
          }
        }
      }
    }
    super.initialize(conf)
  }


  override def start(): Unit = {
    super.start()
    if (UserGroupInformation.isSecurityEnabled) {
      executor.scheduleAtFixedRate(tgtRenewalTask, 0, refreshInterval, TimeUnit.MILLISECONDS)
    }
  }

  override def stop(): Unit = {
    super.stop()
    executor.shutdown()
    try {
      executor.awaitTermination(10, TimeUnit.SECONDS)
    } catch {
      case _: InterruptedException =>
    }
  }
}
