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

package org.apache.kyuubi.server.http.util

import java.security.{MessageDigest, NoSuchAlgorithmException}
import java.util.Base64

import org.apache.kyuubi.Logging

object CookieSigner {
  private val SIGNATURE = "&s="
  private val SHA_STRING = "SHA-256"
}

class CookieSigner(secret: Array[Byte]) extends Logging {
  private val secretBytes = secret.clone()

  /**
   * Sign the cookie given the string token as input.
   *
   * @param str Input token
   * @return Signed token that can be used to create a cookie
   */
  def signCookie(str: String): String = {
    if (str == null || str.isEmpty) {
      throw new IllegalArgumentException("NULL or empty string to sign")
    }
    val signature = getSignature(str)
    debug("Signature generated for " + str + " is " + signature)
    str + CookieSigner.SIGNATURE + signature
  }

  /**
   * Verify a signed string and extracts the original string.
   *
   * @param signedStr The already signed string
   * @return Raw Value of the string without the signature
   */
  def verifyAndExtract(signedStr: String): String = {
    val index = signedStr.lastIndexOf(CookieSigner.SIGNATURE)
    if (index == -1) throw new IllegalArgumentException("Invalid input sign: " + signedStr)
    val originalSignature = signedStr.substring(index + CookieSigner.SIGNATURE.length)
    val rawValue = signedStr.substring(0, index)
    val currentSignature = getSignature(rawValue)
    debug("Signature generated for " + rawValue + " inside verify is " + currentSignature)
    if (!MessageDigest.isEqual(originalSignature.getBytes, currentSignature.getBytes)) {
      throw new IllegalArgumentException("Invalid sign, original = " + originalSignature +
        " current = " + currentSignature)
    }
    rawValue
  }

  /**
   * Get the signature of the input string based on SHA digest algorithm.
   *
   * @param str Input token
   * @return Signed String
   */
  private def getSignature(str: String) =
    try {
      val md = MessageDigest.getInstance(CookieSigner.SHA_STRING)
      md.update(str.getBytes)
      md.update(secretBytes)
      val digest = md.digest
      Base64.getEncoder.encodeToString(digest)
    } catch {
      case ex: NoSuchAlgorithmException =>
        throw new RuntimeException(
          "Invalid SHA digest String: " +
            CookieSigner.SHA_STRING + " " + ex.getMessage,
          ex)
    }
}
