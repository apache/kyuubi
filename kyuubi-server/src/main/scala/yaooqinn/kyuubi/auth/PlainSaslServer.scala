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

import java.io.IOException
import java.security.Provider
import java.util.{ArrayDeque => JDeque, Map => JMap}
import javax.security.auth.callback._
import javax.security.sasl.{AuthorizeCallback, SaslException, SaslServer, SaslServerFactory}

import yaooqinn.kyuubi.auth.AuthMethods.AuthMethods

object PlainSaslServer {

  val PLAIN_METHOD = "PLAIN"

  class SaslPlainServerFactory extends SaslServerFactory {
    override def createSaslServer(
        mechanism: String,
        protocol: String,
        serverName: String,
        props: JMap[String, _],
        cbh: CallbackHandler): SaslServer = mechanism match {
      case PLAIN_METHOD =>
        try {
          new PlainSaslServer(cbh, protocol)
        } catch {
          case _: SaslException => null
        }
      case _ => null
    }

    override def getMechanismNames(props: JMap[String, _]): Array[String] = Array(PLAIN_METHOD)
  }

  class SaslPlainProvider() extends Provider("KyuubiSaslPlain", 1.0, "Kyuubi Plain SASL provider") {
    put("SaslServerFactory.PLAIN", classOf[SaslPlainServerFactory].getName)
  }
}

class PlainSaslServer(handler: CallbackHandler, authMethod: AuthMethods) extends SaslServer {

  def this(handler: CallbackHandler, authMethodStr: String) = this(
    handler, AuthMethods.getValidAuthMethod(authMethodStr))

  private[this] var user: String = _

  override def getMechanismName: String = PlainSaslServer.PLAIN_METHOD

  @throws[SaslException]
  override def evaluateResponse(response: Array[Byte]): Array[Byte] = {
    try {
      // parse the response
      // message = [authzid] UTF8NUL authcid UTF8NUL passwd'
      val tokenList = new JDeque[String]
      val messageToken: StringBuilder = new StringBuilder
      response.foreach {
        case 0 =>
          tokenList.addLast(messageToken.toString)
          messageToken.setLength(0)
        case b: Byte => messageToken.append(b)
      }
      tokenList.addLast(messageToken.toString)
      // validate response
      if (tokenList.size < 2 || tokenList.size > 3) {
        throw new SaslException("Invalid message format")
      }
      val passwd: String = tokenList.removeLast()
      user = tokenList.removeLast()
      // optional authzid
      var authzId: String = null
      if (tokenList.isEmpty) {
        authzId = user
      } else {
        authzId = tokenList.removeLast()
      }
      if (user == null || user.isEmpty) {
        throw new SaslException("No user name provided")
      }
      if (passwd == null || passwd.isEmpty) {
        throw new SaslException("No password name provided")
      }
      val nameCallback: NameCallback = new NameCallback("User")
      nameCallback.setName(user)
      val pcCallback: PasswordCallback = new PasswordCallback("Password", false)
      pcCallback.setPassword(passwd.toCharArray)
      val acCallback: AuthorizeCallback = new AuthorizeCallback(user, authzId)
      val cbList: Array[Callback] = Array(nameCallback, pcCallback, acCallback)
      handler.handle(cbList)
      if (!acCallback.isAuthorized) {
        throw new SaslException("Authentication failed")
      }
    } catch {
      case eL: IllegalStateException => throw new SaslException("Invalid message format", eL)
      case eI: IOException => throw new SaslException("Error validating the login", eI)
      case eU: UnsupportedCallbackException =>
        throw new SaslException("Error validating the login", eU)
    }
    null
  }

  override def isComplete: Boolean = user != null

  override def getAuthorizationID: String = user

  override def unwrap(incoming: Array[Byte], offset: Int, len: Int): Array[Byte] = {
    throw new UnsupportedOperationException
  }

  override def wrap(outgoing: Array[Byte], offset: Int, len: Int): Array[Byte] = {
    throw new UnsupportedOperationException
  }

  override def getNegotiatedProperty(propName: String): AnyRef = null

  override def dispose(): Unit = {}
}
