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

import org.ietf.jgss.{GSSException, Oid}

import org.apache.kyuubi.Logging

object KerberosUtil extends Logging {
  val GSS_SPNEGO_MECH_OID: Oid = getNumericOidInstance("1.3.6.1.5.5.2")
  val GSS_KRB5_MECH_OID: Oid = getNumericOidInstance("1.2.840.113554.1.2.2")
  val NT_GSS_KRB5_PRINCIPAL_OID: Oid = getNumericOidInstance("1.2.840.113554.1.2.2.1")

  // numeric oids will never generate a GSSException for a malformed oid.
  // use to initialize statics.
  private def getNumericOidInstance(oidName: String) =
    try new Oid(oidName)
    catch {
      case ex: GSSException =>
        throw new IllegalArgumentException(ex)
    }
}
