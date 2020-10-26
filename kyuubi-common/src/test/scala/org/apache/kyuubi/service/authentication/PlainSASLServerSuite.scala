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

import java.util.Collections
import javax.security.auth.callback.{Callback, CallbackHandler}
import javax.security.sasl.{AuthorizeCallback, SaslException}

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.service.authentication.PlainSASLServer.SaslPlainServerFactory

class PlainSASLServerSuite extends KyuubiFunSuite {

  test("SaslPlainServerFactory") {
    val plainServerFactory = new SaslPlainServerFactory()
    val map = Collections.emptyMap[String, String]()
    assert(plainServerFactory.getMechanismNames(map) ===
      Array(PlainSASLServer.PLAIN_METHOD))
    val ch = new CallbackHandler {
      override def handle(callbacks: Array[Callback]): Unit = callbacks.foreach {
        case ac: AuthorizeCallback => ac.setAuthorized(true)
        case _ =>
      }
    }

    val s1 = plainServerFactory.createSaslServer("INVALID", "", "", map, ch)
    assert(s1 === null)
    val s2 = plainServerFactory.createSaslServer(PlainSASLServer.PLAIN_METHOD, "", "", map, ch)
    assert(s2 === null)

    val server = plainServerFactory.createSaslServer(
      PlainSASLServer.PLAIN_METHOD, AuthMethods.NONE.toString, "KYUUBI", map, ch)
    assert(server.getMechanismName === PlainSASLServer.PLAIN_METHOD)
    assert(!server.isComplete)

    val e1 = intercept[SaslException](server.evaluateResponse(Array()))
    assert(e1.getMessage === "Error validating the login")
    assert(e1.getCause.getMessage === "Invalid message format")
    val e2 = intercept[SaslException](server.evaluateResponse(Array(0)))
    assert(e2.getMessage === "Error validating the login")
    assert(e2.getCause.getMessage === "No user name provided")
    val e3 = intercept[SaslException](server.evaluateResponse(Array(1, 0)))
    assert(e3.getMessage === "Error validating the login")
    assert(e3.getCause.getMessage === "No password name provided")
    val res4 = Array(97, 0, 99)
    server.evaluateResponse(Array(97, 0, 99))
    assert(server.isComplete)
    assert(server.getAuthorizationID === "a")
    intercept[UnsupportedOperationException](server.wrap(res4.map(_.toByte), 0, 3))
    intercept[UnsupportedOperationException](server.unwrap(res4.map(_.toByte), 0, 3))
    assert(server.getNegotiatedProperty("name") === null)
    val res5 = Array(1, 0, 1, 0, 1, 0)
    val e5 = intercept[SaslException](server.evaluateResponse(Array(1, 0, 1, 0, 1, 0)))
    assert(e5.getMessage === "Error validating the login")
    assert(e5.getCause.getMessage === "Invalid message format")
    assert(server.dispose().isInstanceOf[Unit])

    val server2 = plainServerFactory.createSaslServer(
      "PLAIN",
      "NONE",
      "KYUUBI",
      map, new CallbackHandler {
        override def handle(callbacks: Array[Callback]): Unit = {}
      }
    )
    val e6 = intercept[SaslException](server2.evaluateResponse(res4.map(_.toByte)))
    assert(e6.getMessage === "Error validating the login")
    assert(e6.getCause.getMessage === "Authentication failed")
  }
}
