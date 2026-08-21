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

package org.apache.kyuubi.server.flight

import java.util
import java.util.Base64

import org.apache.arrow.flight.CallHeaders

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.AUTHENTICATION_METHOD

class KyuubiFlightAuthHandlerSuite extends KyuubiFunSuite {

  test("anonymous auth when authentication is disabled") {
    val conf = KyuubiConf().set(AUTHENTICATION_METHOD, Seq("NONE"))
    val auth = new KyuubiFlightAuthHandler(conf)
    val headers = new MapCallHeaders
    assert(auth.authenticate(headers).getPeerIdentity === "anonymous")

    headers.insert("x-user-name", "alice")
    assert(auth.authenticate(headers).getPeerIdentity === "alice")
  }

  test("missing credentials fail when auth is required") {
    val conf = KyuubiConf().set(AUTHENTICATION_METHOD, Seq("LDAP"))
    val auth = new KyuubiFlightAuthHandler(conf)
    intercept[RuntimeException] {
      auth.authenticate(new MapCallHeaders)
    }
  }

  test("malformed basic credentials are rejected") {
    val conf = KyuubiConf().set(AUTHENTICATION_METHOD, Seq("LDAP"))
    val auth = new KyuubiFlightAuthHandler(conf)
    val headers = new MapCallHeaders
    headers.insert(
      "authorization",
      "Basic " + Base64.getEncoder.encodeToString("nouser".getBytes("UTF-8")))
    intercept[RuntimeException] {
      auth.authenticate(headers)
    }
  }

  private class MapCallHeaders extends CallHeaders {
    private val values = new util.LinkedHashMap[String, String]()

    override def get(key: String): String = values.get(key.toLowerCase)

    override def getByte(key: String): Array[Byte] = null

    override def getAll(key: String): java.lang.Iterable[String] = {
      val value = values.get(key.toLowerCase)
      if (value == null) util.Collections.emptyList()
      else util.Collections.singletonList(value)
    }

    override def getAllByte(key: String): java.lang.Iterable[Array[Byte]] =
      util.Collections.emptyList()

    override def insert(key: String, value: String): Unit =
      values.put(key.toLowerCase, value)

    override def insert(key: String, value: Array[Byte]): Unit = ()

    override def keys(): util.Set[String] = values.keySet()

    override def containsKey(key: String): Boolean = values.containsKey(key.toLowerCase)
  }
}
