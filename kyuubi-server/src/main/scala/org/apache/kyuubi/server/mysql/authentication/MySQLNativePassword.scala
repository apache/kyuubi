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

package org.apache.kyuubi.server.mysql.authentication

import java.util

import org.apache.commons.codec.digest.DigestUtils

import org.apache.kyuubi.server.mysql.authentication.MySQLAuthentication.randomBytes
import org.apache.kyuubi.server.mysql.authentication.MySQLNativePassword.PluginData
import org.apache.kyuubi.server.mysql.constant.MySQLErrorCode

object MySQLNativePassword {
  case class PluginData(
      part1: Array[Byte] = randomBytes(8),
      part2: Array[Byte] = randomBytes(12)
  ) {
    lazy val full: Array[Byte] = Array.concat(part1, part2)
  }
}

class MySQLNativePassword {
  private final val _pluginData = new PluginData

  def pluginData: PluginData = _pluginData

  def login(
      user: String,
      host: String,
      authResp: Array[Byte],
      database: String
  ): Option[MySQLErrorCode] = {
    if (isPasswordRight("kyuubi", authResp)) {
      // if (true) {
      None
    } else {
      Some(MySQLErrorCode.ER_ACCESS_DENIED_ERROR)
    }
  }

  private[authentication] def isPasswordRight(password: String, authentication: Array[Byte]) =
    util.Arrays.equals(getAuthCipherBytes(password), authentication)

  private def getAuthCipherBytes(password: String): Array[Byte] = {
    val salt = pluginData.full
    val passwordSha1 = DigestUtils.sha1(password)
    val passwordSha1Sha1 = DigestUtils.sha1(passwordSha1)
    val secret = new Array[Byte](salt.length + passwordSha1Sha1.length)
    System.arraycopy(salt, 0, secret, 0, salt.length)
    System.arraycopy(passwordSha1Sha1, 0, secret, salt.length, passwordSha1Sha1.length)
    val secretSha1 = DigestUtils.sha1(secret)
    xor(passwordSha1, secretSha1)
  }

  private def xor(input: Array[Byte], secret: Array[Byte]): Array[Byte] =
    (input zip secret).map { case (b1, b2) => (b1 ^ b2).toByte }
}
