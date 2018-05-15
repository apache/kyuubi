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

package yaooqinn.kyuubi.auth


import java.io.{File, IOException}
import java.util.UUID

import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge
import org.apache.hadoop.minikdc.MiniKdc
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.cli.thrift.TCLIService.Client
import org.apache.spark.{KyuubiConf, SparkConf, SparkFunSuite, KyuubiSparkUtil}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket

import yaooqinn.kyuubi.auth.KerberosSaslHelper.CLIServiceProcessorFactory
import yaooqinn.kyuubi.utils.ReflectUtils

class KerberosTest extends SparkFunSuite {

  var kdc: MiniKdc = _
  val baseDir = KyuubiSparkUtil.createTempDir(namePrefix = "kyuubi-kdc")

  override def beforeAll() {
    super.beforeAll()
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
  }

  override def afterAll() {
    super.afterAll()
    if (kdc != null) {
      kdc.stop()
    }
  }

  /**
   * Get realm.
   */
  def getRealm: String = kdc.getRealm

  def createRandomKeytab(userName: String = null): (String, String) = {
    val user = if (userName == null) {
      UserGroupInformation.getLoginUser.getShortUserName
    } else {
      userName
    }
    val keytab = new File(baseDir, user + ".keytab")
    val princ = user + "/localhost"
    kdc.createPrincipal(keytab, princ)
    (princ, keytab.getAbsolutePath)
  }

  ignore("kerberos sasl server exists") {
    val (princ, keytab) = createRandomKeytab("alice")
    val conf = new SparkConf()

    conf.set(KyuubiConf.AUTHENTICATION_METHOD.key, "KERBEROS")
    conf.set(KyuubiSparkUtil.KEYTAB, keytab)
    conf.set(KyuubiSparkUtil.PRINCIPAL, princ)
    conf.set("spark.hadoop.hadoop.security.authentication", "KERBEROS")
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    UserGroupInformation.setConfiguration(hadoopConf)
    KyuubiConf.getAllDefaults.foreach(c => conf.setIfMissing(c._1, c._2))
    val authz = new KyuubiAuthFactory(conf)
    assert(ReflectUtils
      .getFieldValue(authz, "saslServer")
      .asInstanceOf[Option[HadoopThriftAuthBridge.Server]].nonEmpty)
  }

  ignore("KerberosSaslHelper testGetProcessorFactory") {
    val (princ, keytab) = createRandomKeytab("bob")
    val server =
      ShimLoader.getHadoopThriftAuthBridge.createServer(keytab, princ)
    val tSocket = new TSocket("0.0.0.0", 0)
    val tProtocol = new TBinaryProtocol(tSocket)
    val tProcessorFactory = KerberosSaslHelper.getProcessorFactory(server, new Client(tProtocol))
    assert(tProcessorFactory.isInstanceOf[CLIServiceProcessorFactory])
  }
}
