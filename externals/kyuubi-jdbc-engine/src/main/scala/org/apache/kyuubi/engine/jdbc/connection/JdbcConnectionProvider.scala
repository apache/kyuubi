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
package org.apache.kyuubi.engine.jdbc.connection

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.jdbc.util.SupportServiceLoader

abstract class JdbcConnectionProvider extends SupportServiceLoader with Logging {

  override def name(): String = classOf[JdbcConnectionProvider].getName

  val driverClass: String

  def canHandle(providerClass: String): Boolean = {
    driverClass.equalsIgnoreCase(providerClass)
  }

  def getConnection(kyuubiConf: KyuubiConf): Connection = {
    val properties = new Properties()
    val url = kyuubiConf.get(ENGINE_JDBC_CONNECTION_URL).get
    val user = kyuubiConf.get(ENGINE_JDBC_CONNECTION_USER)
    if (user.isDefined) {
      properties.setProperty("user", user.get)
    }

    val password = kyuubiConf.get(ENGINE_JDBC_CONNECTION_PASSWORD)
    if (password.isDefined) {
      properties.setProperty("password", password.get)
    }
    kyuubiConf.get(ENGINE_JDBC_CONNECTION_PROPERTIES).foreach { prop =>
      val tuple = prop.split("=", 2).map(_.trim)
      properties.setProperty(tuple(0), tuple(1))
    }
    info(s"Starting to get connection to $url")
    val connection = DriverManager.getConnection(url, properties)
    info(s"Got the connection to $url")
    connection
  }
}
