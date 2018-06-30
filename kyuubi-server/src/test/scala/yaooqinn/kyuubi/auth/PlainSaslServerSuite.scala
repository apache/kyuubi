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

import javax.security.auth.callback.{Callback, CallbackHandler}
import javax.security.sasl.{AuthorizeCallback, SaslException}

import scala.collection.JavaConverters._

import org.apache.spark.SparkFunSuite

import yaooqinn.kyuubi.auth

class PlainSaslServerSuite extends SparkFunSuite {

  test("PLAIN_METHOD Const") {
    assert(PlainSaslServer.PLAIN_METHOD === "PLAIN")
  }

  test("Sasl Plain Server Factory ") {
    val saslPlainServerFactory = new auth.PlainSaslServer.SaslPlainServerFactory
    val tTransportFactory = PlainSaslHelper.getTransportFactory("NONE")

    val emp = Map.empty[String, String].asJava
    assert(saslPlainServerFactory.getMechanismNames(emp).head ===
      PlainSaslServer.PLAIN_METHOD)
    val server = saslPlainServerFactory.createSaslServer(
      "PLAIN",
      "NONE",
      "KYUUBI",
      emp, new CallbackHandler {
        override def handle(callbacks: Array[Callback]): Unit = callbacks.foreach {
          case ac: AuthorizeCallback => ac.setAuthorized(true)
          case _ =>
        }
      }
    )
    assert(server.isInstanceOf[PlainSaslServer])
    assert(server.getMechanismName === PlainSaslServer.PLAIN_METHOD)
    assert(!server.isComplete)

    val res1 = Array.empty[Int]
    val e1 = intercept[SaslException](server.evaluateResponse(res1.map(_.toByte)))
    assert(e1.getMessage === "Error validating the login")
    assert(e1.getCause.getMessage === "Invalid message format")
    val res2 = Array(0)
    val e2 = intercept[SaslException](server.evaluateResponse(res2.map(_.toByte)))
    assert(e2.getMessage === "Error validating the login")
    assert(e2.getCause.getMessage === "No user name provided")
    val res3 = Array(1, 0)
    val e3 = intercept[SaslException](server.evaluateResponse(res3.map(_.toByte)))
    assert(e3.getMessage === "Error validating the login")
    assert(e3.getCause.getMessage === "No password name provided")
    val res4 = Array(1, 0, 1)
    server.evaluateResponse(res4.map(_.toByte))

  }

}
