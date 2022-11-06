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

package org.apache.kyuubi.beeline

import java.util.{Map => JMap}
import java.util.Locale

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.apache.kyuubi.config.{ConfigBuilder, ConfigEntry, KyuubiConf, OptionalConfigEntry}
import org.apache.kyuubi.service.authentication.{AuthTypes, SaslQOP}

object BeelineConf {

  final val BEELINE_SESSION_CONF_PREFIX = "kyuubi.beeline.sessionConf"
  final val BEELINE_KYUUBI_CONF_PREFIX = "kyuubi.beeline.kyuubiConf"
  final val BEELINE_KYUUBI_VAR_PREFIX = "kyuubi.beeline.kyuubiVar"

  def getBeelineSessionConfs(conf: KyuubiConf): JMap[String, String] = {
    conf.getAllWithPrefix(BEELINE_SESSION_CONF_PREFIX, "").asJava
  }

  def getBeelineKyuubiConfs(conf: KyuubiConf): JMap[String, String] = {
    conf.getAllWithPrefix(BEELINE_KYUUBI_CONF_PREFIX, "").asJava
  }

  def getBeelineKyuubiVars(conf: KyuubiConf): JMap[String, String] = {
    conf.getAllWithPrefix(BEELINE_KYUUBI_VAR_PREFIX, "").asJava
  }

  private def buildConf(key: String): ConfigBuilder = KyuubiConf.buildConf(key)

  val BEELINE_DB_NAME: ConfigEntry[String] = buildConf("kyuubi.beeline.db.name")
    .doc("Optional database name to set the current database to" +
      " run the query against, use `default` if absent.")
    .version("1.7.0")
    .stringConf
    .createWithDefault("default")

  val BEELINE_THRIFT_TRANSPORT_MODE: ConfigEntry[String] =
    buildConf("kyuubi.beeline.thrift.transport.mode")
      .doc("Transport mode of HiveServer2: " +
        "<ul>" +
        " <li>THRIFT_BINARY - HiveServer2 compatible thrift binary protocol.</li>" +
        " <li>THRIFT_HTTP - HiveServer2 compatible thrift http protocol.</li>" +
        "</ul>")
      .version("1.7.0")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .createWithDefault("thrift_binary")

  val BEELINE_THRIFT_BINARY_BIND_HOST: OptionalConfigEntry[String] =
    buildConf("kyuubi.beeline.thrift.binary.bind.host")
      .doc("Hostname or IP of the machine on which to run the thrift frontend service " +
        "via binary protocol.")
      .version("1.7.0")
      .stringConf
      .createOptional

  val BEELINE_THRIFT_BINARY_BIND_PORT: ConfigEntry[Int] =
    buildConf("kyuubi.beeline.thrift.binary.bind.port")
      .doc("Port of the machine on which to run the thrift frontend service " +
        "via binary protocol.")
      .version("1.7.0")
      .intConf
      .checkValue(p => p == 0 || (p > 1024 && p < 65535), "Invalid Port number")
      .createWithDefault(10009)

  val BEELINE_THRIFT_HTTP_BIND_HOST: OptionalConfigEntry[String] =
    buildConf("kyuubi.beeline.thrift.http.bind.host")
      .doc("Hostname or IP of the machine on which to run the thrift frontend service " +
        "via http protocol.")
      .version("1.7.0")
      .stringConf
      .createOptional

  val BEELINE_THRIFT_HTTP_BIND_PORT: ConfigEntry[Int] =
    buildConf("kyuubi.beeline.thrift.http.bind.port")
      .doc("Port of the machine on which to run the thrift frontend service " +
        "via http protocol.")
      .version("1.7.0")
      .intConf
      .checkValue(p => p == 0 || (p > 1024 && p < 65535), "Invalid Port number")
      .createWithDefault(10010)

  val BEELINE_THRIFT_HTTP_PATH: ConfigEntry[String] =
    buildConf("kyuubi.beeline.thrift.http.path")
      .doc("Path component of URL endpoint when in HTTP mode.")
      .version("1.7.0")
      .stringConf
      .createWithDefault("cliservice")

  val BEELINE_AUTHENTICATION_METHOD: ConfigEntry[Seq[String]] =
    buildConf("kyuubi.beeline.authentication")
      .doc("A comma separated list of client authentication types.<ul>" +
        " <li>NOSASL: raw transport.</li>" +
        " <li>NONE: no authentication check.</li>" +
        " <li>KERBEROS: Kerberos/GSSAPI authentication.</li>" +
        " <li>CUSTOM: User-defined authentication.</li>" +
        " <li>JDBC: JDBC query authentication.</li>" +
        " <li>LDAP: Lightweight Directory Access Protocol authentication.</li>" +
        "</ul>" +
        " Note that: For KERBEROS, it is SASL/GSSAPI mechanism," +
        " and for NONE, CUSTOM and LDAP, they are all SASL/PLAIN mechanism." +
        " If only NOSASL is specified, the authentication will be NOSASL." +
        " For SASL authentication, KERBEROS and PLAIN auth type are supported at the same time," +
        " and only the first specified PLAIN auth type is valid.")
      .version("1.7.0")
      .stringConf
      .toSequence()
      .transform(_.map(_.toUpperCase(Locale.ROOT)))
      .checkValue(
        _.forall(AuthTypes.values.map(_.toString).contains),
        s"the authentication type should be one or more of ${AuthTypes.values.mkString(",")}")
      .createWithDefault(Seq(AuthTypes.NONE.toString))

  val BEELINE_KERBEROS_PRINCIPAL: ConfigEntry[String] =
    buildConf("kyuubi.beeline.kerberos.principal")
      .doc("Name of the Kerberos principal.")
      .version("1.7.0")
      .stringConf
      .createWithDefault("")

  val BEELINE_HA_ADDRESSES: ConfigEntry[String] = buildConf("kyuubi.beeline.ha.addresses")
    .doc("The connection string for the discovery ensemble.")
    .version("1.7.0")
    .stringConf
    .createWithDefault("")

  val BEELINE_HA_NAMESPACE: ConfigEntry[String] = buildConf("kyuubi.beeline.ha.namespace")
    .doc("The root directory for the service to deploy its instance uri.")
    .version("1.7.0")
    .stringConf
    .createWithDefault("kyuubi")

  val BEELINE_USE_SSL: ConfigEntry[Boolean] = buildConf("kyuubi.beeline.use.SSL")
    .doc("Set this to true for using SSL encryption.")
    .version("1.7.0")
    .booleanConf
    .createWithDefault(false)

  val BEELINE_SSL_TRUSTSTORE: OptionalConfigEntry[String] =
    buildConf("kyuubi.beeline.ssl.truststore")
      .doc("The path to the SSL TrustStore.")
      .version("1.7.0")
      .stringConf
      .createOptional

  val BEELINE_SSL_TRUSTSTORE_PASSWORD: OptionalConfigEntry[String] =
    buildConf("kyuubi.beeline.ssl.truststore.password")
      .doc("The password to the SSL TrustStore.")
      .version("1.7.0")
      .stringConf
      .createOptional

  val BEELINE_SASL_QOP: ConfigEntry[String] = buildConf("kyuubi.beeline.sasl.qop")
    .doc("Sasl QOP enable higher levels of protection for Kyuubi communication with " +
      "clients.<ul>" +
      " <li>auth - authentication only (default)</li>" +
      " <li>auth-int - authentication plus integrity protection</li>" +
      " <li>auth-conf - authentication plus integrity and confidentiality protection. This is" +
      " applicable only if Kyuubi is configured to use Kerberos authentication.</li> </ul>")
    .version("1.7.0")
    .stringConf
    .checkValues(SaslQOP.values.map(_.toString))
    .transform(_.toLowerCase(Locale.ROOT))
    .createWithDefault(SaslQOP.AUTH.toString)

}
