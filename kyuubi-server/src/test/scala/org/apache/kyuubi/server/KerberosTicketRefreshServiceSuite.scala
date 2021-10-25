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

import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.KerberizedTestHelper
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.ServiceState

class KerberosTicketRefreshServiceSuite extends KerberizedTestHelper {

  test("non secured kerberos ticket refresh service") {
    val service = new KerberosTicketRefreshService()
    assert(service.getServiceState === ServiceState.LATENT)
    service.initialize(KyuubiConf())
    assert(service.getServiceState === ServiceState.INITIALIZED)
    service.start()
    assert(service.getServiceState === ServiceState.STARTED)
    service.stop()
    assert(service.getServiceState === ServiceState.STOPPED)
  }

  test("secured kerberos ticket refresh service") {
    tryWithSecurityEnabled {
      val service = new KerberosTicketRefreshService()
      val conf = KyuubiConf()
      val e = intercept[IllegalArgumentException](service.initialize(conf))
      assert(e.getMessage === "requirement failed: principal or keytab is missing")
      conf.set(KyuubiConf.SERVER_PRINCIPAL, testPrincipal)
        .set(KyuubiConf.SERVER_KEYTAB, testKeytab)
        .set(KyuubiConf.KINIT_INTERVAL, 0L)
      service.initialize(conf)
      assert(UserGroupInformation.getCurrentUser.hasKerberosCredentials)
      assert(UserGroupInformation.getCurrentUser.isFromKeytab)
      service.start()
      assert(service.getServiceState === ServiceState.STARTED)
      service.stop()
      assert(service.getServiceState === ServiceState.STOPPED)
    }
  }

}
