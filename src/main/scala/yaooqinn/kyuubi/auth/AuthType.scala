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

import yaooqinn.kyuubi.service.ServiceException

abstract class AuthType {
  def name: String = ""
  override def toString: String = name

}

object AuthType {

  case object NOSASL extends AuthType {
    override val name: String = "NOSASL"
  }

  case object NONE extends AuthType {
    override val name: String = "NONE"
  }

  case object KERBEROS extends AuthType {
    override val name: String = "KERBEROS"
  }

  def toAuthType(authTypeStr: String): AuthType = authTypeStr.toUpperCase match {
    case NOSASL.name => NOSASL
    case NONE.name => NONE
    case KERBEROS.name => KERBEROS
    case other => throw new ServiceException("Unsupported authentication method: " + other)
  }
}
