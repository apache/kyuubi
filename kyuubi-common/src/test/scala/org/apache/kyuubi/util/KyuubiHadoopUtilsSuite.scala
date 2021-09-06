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

package org.apache.kyuubi.util

import scala.util.Random

import org.apache.hadoop.io.Text
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.token.Token

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.KyuubiDelegationTokenIdentifier

class KyuubiHadoopUtilsSuite extends KyuubiFunSuite {

  test("new hadoop conf with kyuubi conf") {
    val abc = "hadoop.abc"
    val xyz = "hadoop.xyz"
    val test = "hadoop.test"
    val kyuubiConf = new KyuubiConf()
      .set(abc, "xyz")
      .set(xyz, "abc")
      .set(test, "t")
    val hadoopConf = KyuubiHadoopUtils.newHadoopConf(kyuubiConf)
    assert(hadoopConf.get(abc) === "xyz")
    assert(hadoopConf.get(xyz) === "abc")
    assert(hadoopConf.get(test) === "t")
  }

  test("encode/decode credentials") {
    val identifier = new KyuubiDelegationTokenIdentifier()
    val password = new Array[Byte](128)
    Random.nextBytes(password)
    val token = new Token[KyuubiDelegationTokenIdentifier](
      identifier.getBytes,
      password,
      identifier.getKind,
      new Text(""))
    val credentials = new Credentials()
    credentials.addToken(token.getKind, token)

    val decoded = KyuubiHadoopUtils.decodeCredentials(
      KyuubiHadoopUtils.encodeCredentials(credentials))
    assert(decoded.getToken(token.getKind) == credentials.getToken(token.getKind))
  }
}
