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

import java.util.Locale


trait SaslQOP {
  override def toString: String = "auth, auth-int, auth-conf"
}

/**
 * Possible values of SASL quality-of-protection value.
 */
object SaslQOP {

  /** Authentication only. */
  case object AUTH extends SaslQOP {
    override val toString: String = "auth"
  }

  /** Authentication and integrity checking by using signatures. */
  case object AUTH_INT extends SaslQOP {
    override val toString: String = "auth-int"
  }

  /** Authentication, integrity and confidentiality checking by using signatures and encryption. */
  case object AUTH_CONF extends SaslQOP {
    override val toString: String = "auth-conf"
  }

  def fromString(str: String): SaslQOP = str.toLowerCase(Locale.ROOT) match {
    case AUTH.toString => AUTH
    case AUTH_INT.toString => AUTH_INT
    case AUTH_CONF.toString => AUTH_CONF
    case _ =>
      throw new IllegalArgumentException(
        s"Unknown auth type: $str only ${SaslQOP.toString} allowed.")
  }

}