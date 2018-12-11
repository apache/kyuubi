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

import java.security.Security
import javax.security.auth.login.LoginException

import org.apache.spark.{KyuubiSparkUtil, SparkConf, SparkFunSuite}
import org.apache.thrift.transport.{TSaslServerTransport, TSocket}

import yaooqinn.kyuubi.auth.PlainSaslServer.SaslPlainProvider
import yaooqinn.kyuubi.server.KyuubiServer

class PlainSaslHelperSuite extends SparkFunSuite {

  test("Plain Sasl Helper") {
    val conf = new SparkConf(loadDefaults = true)
    KyuubiSparkUtil.setupCommonConfig(conf)
    val server = new KyuubiServer()
    val fe = server.feService
    val tProcessorFactory = PlainSaslHelper.getProcessFactory(fe)
    assert(!tProcessorFactory.isAsyncProcessor)
    val tSocket = new TSocket("0.0.0.0", 0)
    val tProcessor = tProcessorFactory.getProcessor(tSocket)
    assert(tProcessor.isInstanceOf[TSetIpAddressProcessor[_]])
    intercept[LoginException](PlainSaslHelper.getTransportFactory("KERBEROS", conf))
    intercept[LoginException](PlainSaslHelper.getTransportFactory("NOSASL", conf))
    intercept[LoginException](PlainSaslHelper.getTransportFactory("ELSE", conf))
    val tTransportFactory = PlainSaslHelper.getTransportFactory("NONE", conf)
    assert(tTransportFactory.isInstanceOf[TSaslServerTransport.Factory])
    Security.getProviders.exists(_.isInstanceOf[SaslPlainProvider])
  }

  test("Sasl Plain Provider") {
    val saslPlainProvider = new SaslPlainProvider()
    assert(saslPlainProvider.containsKey("SaslServerFactory.PLAIN"))
    assert(saslPlainProvider.getName === "KyuubiSaslPlain")

  }


}
