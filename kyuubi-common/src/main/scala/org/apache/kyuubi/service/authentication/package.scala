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

package org.apache.kyuubi.service

import java.time.Duration
import java.util.Locale

import org.apache.kyuubi.config.{ConfigEntry, OptionalConfigEntry}
import org.apache.kyuubi.config.KyuubiConf._

package object authentication {

  val AUTHENTICATION_METHOD: ConfigEntry[String] = buildConf("authentication")
    .doc("Client authentication types." +
      " NONE: no authentication check." +
      " KERBEROS: Kerberos/GSSAPI authentication." +
      " LDAP: Lightweight Directory Access Protocol authentication.")
    .version("1.0.0")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValues(AuthTypes.values.map(_.toString))
    .createWithDefault(AuthTypes.NONE.toString)

  val AUTHENTICATION_LDAP_URL: OptionalConfigEntry[String] = buildConf("authentication.ldap.url")
    .doc("SPACE character separated LDAP connection URL(s).")
    .version("1.0.0")
    .stringConf
    .createOptional

  val AUTHENTICATION_LDAP_BASEDN: OptionalConfigEntry[String] =
    buildConf("authentication.ldap.base.dn")
      .doc("LDAP base DN.")
      .version("1.0.0")
      .stringConf
      .createOptional

  val AUTHENTICATION_LDAP_DOMAIN: OptionalConfigEntry[String] =
    buildConf("authentication.ldap.domain")
      .doc("LDAP base DN.")
      .version("1.0.0")
      .stringConf
      .createOptional

  val DELEGATION_KEY_UPDATE_INTERVAL: ConfigEntry[Long] =
    buildConf("delegation.key.update.interval")
      .doc("")
      .version("1.0.0")
      .timeConf
      .createWithDefault(Duration.ofDays(1).toMillis)

  val DELEGATION_TOKEN_MAX_LIFETIME: ConfigEntry[Long] =
    buildConf("delegation.token.max.lifetime")
      .doc("")
      .version("1.0.0")
      .timeConf
      .createWithDefault(Duration.ofDays(7).toMillis)

  val DELEGATION_TOKEN_GC_INTERVAL: ConfigEntry[Long] =
    buildConf("delegation.token.gc.interval")
      .doc("")
      .version("1.0.0")
      .timeConf
      .createWithDefault(Duration.ofHours(1).toMillis)

  val DELEGATION_TOKEN_RENEW_INTERVAL: ConfigEntry[Long] =
    buildConf("delegation.token.renew.interval")
      .doc("")
      .version("1.0.0")
      .timeConf
      .createWithDefault(Duration.ofDays(7).toMillis)

  val SASL_QOP: ConfigEntry[String] = buildConf("sasl.qop")
    .doc("Sasl QOP enable higher levels of protection for Kyuubi communication with clients." +
      " auth - authentication only (default)" +
      " auth-int - authentication plus integrity protection" +
      " auth-conf - authentication plus integrity and confidentiality protectionThis is" +
      " applicable only if Kyuubi is configured to use Kerberos authentication.")
    .version("1.0.0")
    .stringConf
    .transform(_.toLowerCase(Locale.ROOT))
    .createWithDefault(SaslQOP.AUTH.toString)
}
