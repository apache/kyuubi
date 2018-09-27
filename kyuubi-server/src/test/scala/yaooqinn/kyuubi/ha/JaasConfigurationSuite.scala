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

import javax.security.auth.login.Configuration

import org.apache.spark.{KyuubiSparkUtil, SparkFunSuite}
import org.scalatest.Matchers

class JaasConfigurationSuite extends SparkFunSuite with Matchers {

  test("Get App Configuration Entry") {

    val loginContext = "KYUUBI_AS_CLIENT"
    val principal = KyuubiSparkUtil.getCurrentUserName
    val keytab = ""
    val conf = new JaasConfiguration(loginContext, principal, keytab)
    val options = conf.getAppConfigurationEntry(loginContext).head.getOptions
    options.get("principal") should be(principal)
    conf.getAppConfigurationEntry("no name") should be(null)

    Configuration.setConfiguration(conf)
    val conf2 = new JaasConfiguration(loginContext, principal, keytab)

    conf2.getAppConfigurationEntry("no name") should be(null)

    val options2 = conf2.getAppConfigurationEntry(loginContext).head.getOptions
    options2.get("principal") should be(principal)
  }

}
