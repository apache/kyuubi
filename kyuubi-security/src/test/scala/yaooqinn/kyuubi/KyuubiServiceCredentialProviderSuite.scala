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

import java.net.InetAddress

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.yarn.security.ServiceCredentialProvider
import org.scalatest.mock.MockitoSugar

class KyuubiServiceCredentialProviderSuite extends SparkFunSuite with MockitoSugar {
  test("obtain credentials") {
    val sparkConf = new SparkConf()
    val userName = UserGroupInformation.getCurrentUser.getShortUserName
    val hadoopConf = new Configuration()
    val credential = new Credentials()
    val provider = new KyuubiServiceCredentialProvider
    val e1 =
      intercept[RuntimeException](provider.obtainCredentials(hadoopConf, sparkConf, credential))
    assert(e1.getMessage === "Can't get Master Kerberos principal for use as renewer.")
    hadoopConf.set(YarnConfiguration.RM_PRINCIPAL, "")
    val e2 =
      intercept[RuntimeException](provider.obtainCredentials(hadoopConf, sparkConf, credential))
    assert(e2.getMessage === "Can't get Master Kerberos principal for use as renewer.")

    hadoopConf.set(YarnConfiguration.RM_PRINCIPAL,
      userName + "/" + InetAddress.getLocalHost.getHostName + "@" + "KYUUBI.ORG")
    assert(provider.isInstanceOf[ServiceCredentialProvider])
    assert(provider.isInstanceOf[Logging])
    assert(!provider.credentialsRequired(hadoopConf))
    assert(provider.serviceName === "kyuubi")
    val now = System.currentTimeMillis()
    val renewalTime = provider.obtainCredentials(hadoopConf, sparkConf, credential)
    assert(renewalTime.isDefined)
    assert(renewalTime.get - now >= 2L * 60 * 60 *100 )
    sparkConf.set("spark.yarn.access.hadoopFileSystems", "hdfs://a/b/c")
    intercept[Exception](provider.obtainCredentials(hadoopConf, sparkConf, credential))
    sparkConf.set("spark.yarn.access.namenodes", "hdfs://a/b/c, hdfs://d/e/f")
    intercept[Exception](provider.obtainCredentials(hadoopConf, sparkConf, credential))
  }
}
