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

package org.apache.kyuubi.service.authentication

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.AUTHENTICATION_METHOD
import org.apache.kyuubi.service.authentication.AuthTypes.{KERBEROS, NOSASL}

class KyuubiRestAuthenticationFactory(conf: KyuubiConf) extends Logging {
  private val authTypes = conf.get(AUTHENTICATION_METHOD).map(AuthTypes.withName)
  private val spnegoKerberosEnabled = authTypes.contains(KERBEROS)
  private val basicAuthTypeOpt =
    if (authTypes == Seq(NOSASL)) {
      authTypes.headOption
    } else {
      authTypes.filterNot(_.equals(KERBEROS)).filterNot(_.equals(NOSASL)).headOption
    }

  def initHttpAuthenticationFilter(): Unit = {
    if (spnegoKerberosEnabled) {
      AuthenticationFilter.addAuthHandler(new KerberosAuthenticationHandler, conf)
    }

    basicAuthTypeOpt.foreach { basicAuthType =>
      AuthenticationFilter.addAuthHandler(new BasicAuthenticationHandler(basicAuthType), conf)
    }
  }
}
