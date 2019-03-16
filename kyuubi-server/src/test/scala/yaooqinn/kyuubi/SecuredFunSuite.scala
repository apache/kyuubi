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

package yaooqinn.kyuubi

import java.io.IOException

import org.apache.hadoop.minikdc.MiniKdc
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{KyuubiSparkUtil, SparkConf}

trait SecuredFunSuite {

  var kdc: MiniKdc = null
  val baseDir = KyuubiSparkUtil.createTempDir("kyuubi-kdc")
  try {
    val kdcConf = MiniKdc.createConf()
    kdcConf.setProperty(MiniKdc.INSTANCE, "KyuubiKrbServer")
    kdcConf.setProperty(MiniKdc.ORG_NAME, "KYUUBI")
    kdcConf.setProperty(MiniKdc.ORG_DOMAIN, "COM")

    if (kdc == null) {
      kdc = new MiniKdc(kdcConf, baseDir)
      kdc.start()
    }
  } catch {
    case e: IOException =>
      throw new AssertionError("unable to create temporary directory: " + e.getMessage)
  }

  def tryWithSecurityEnabled(block: => Unit): Unit = {
    val conf = new SparkConf(true)
    assert(!UserGroupInformation.isSecurityEnabled)
    val authType = "spark.hadoop.hadoop.security.authentication"
    try {
      conf.set(authType, "KERBEROS")
      System.setProperty("java.security.krb5.realm", kdc.getRealm)
      UserGroupInformation.setConfiguration(KyuubiSparkUtil.newConfiguration(conf))
      assert(UserGroupInformation.isSecurityEnabled)
      block
    } finally {
      conf.remove(authType)
      System.clearProperty("java.security.krb5.realm")
      UserGroupInformation.setConfiguration(KyuubiSparkUtil.newConfiguration(conf))
      assert(!UserGroupInformation.isSecurityEnabled)
    }
  }
}
