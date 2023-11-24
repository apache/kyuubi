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

import java.sql.{Connection, Driver, DriverManager}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_JDBC_CONNECTION_PROVIDER, ENGINE_JDBC_CONNECTION_URL, ENGINE_JDBC_DRIVER_CLASS}
import org.apache.kyuubi.util.reflect.DynClasses
import org.apache.kyuubi.util.reflect.ReflectUtils._

abstract class AbstractConnectionProvider extends Logging {
  protected val providers = loadProviders()

  def getDriverClass(kyuubiConf: KyuubiConf): String = {
    val driverClass: Class[_ <: Driver] = Option(
      DynClasses.builder().impl(kyuubiConf.get(ENGINE_JDBC_DRIVER_CLASS).get)
        .orNull().build[Driver]()).getOrElse {
      val url = kyuubiConf.get(ENGINE_JDBC_CONNECTION_URL).get
      DriverManager.getDriver(url).getClass
    }
    driverClass.getCanonicalName
  }

  def create(kyuubiConf: KyuubiConf): Connection = {
    val filteredProviders = providers.filter(_.canHandle(getDriverClass(kyuubiConf)))
    if (filteredProviders.isEmpty) {
      throw new IllegalArgumentException(
        "Empty list of JDBC connection providers for the specified driver and options")
    }

    val connectionProviderName = kyuubiConf.get(ENGINE_JDBC_CONNECTION_PROVIDER)
    val selectedProvider = connectionProviderName match {
      case Some(providerName) =>
        // It is assumed that no two providers will have the same name
        filteredProviders.find(_.name == providerName).getOrElse {
          throw new IllegalArgumentException(
            s"Could not find a JDBC connection provider with name '$providerName' " +
              "that can handle the specified driver and options. " +
              s"Available providers are ${providers.mkString("[", ", ", "]")}")
        }
      case None =>
        // TODO
        if (filteredProviders.size != 1) {
          warn("JDBC connection initiated but more than one connection provider was found. Use " +
            s"${ENGINE_JDBC_CONNECTION_PROVIDER.key} option to select a specific provider. " +
            s"Found active providers ${filteredProviders.mkString("[", ", ", "]")}")
        }
        filteredProviders.head
    }
    // TODO support security connection
    selectedProvider.getConnection(kyuubiConf)
  }

  def loadProviders(): Seq[JdbcConnectionProvider] =
    loadFromServiceLoader[JdbcConnectionProvider]()
      .map { provider =>
        info(s"Loaded provider: $provider")
        provider
      }.toSeq
}

object ConnectionProvider extends AbstractConnectionProvider
