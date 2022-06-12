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
import java.util.ServiceLoader

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_JDBC_CONNECTION_PROVIDER, ENGINE_JDBC_CONNECTION_URL, ENGINE_JDBC_DRIVER_CLASS}

abstract class AbstractConnectionProvider extends Logging {
  protected val providers = loadProviders()

  def getProviderClass(kyuubiConf: KyuubiConf): String = {
    val specifiedDriverClass = kyuubiConf.get(ENGINE_JDBC_DRIVER_CLASS)
    specifiedDriverClass.foreach(Class.forName)

    specifiedDriverClass.getOrElse {
      val url = kyuubiConf.get(ENGINE_JDBC_CONNECTION_URL).get
      DriverManager.getDriver(url).getClass.getCanonicalName
    }
  }

  def create(kyuubiConf: KyuubiConf): Connection = {
    val filteredProviders = providers.filter(_.canHandle(getProviderClass(kyuubiConf)))
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
          throw new IllegalArgumentException(
            "JDBC connection initiated but more than one connection provider was found. Use " +
              s"${ENGINE_JDBC_CONNECTION_PROVIDER.key} option to select a specific provider. " +
              s"Found active providers ${filteredProviders.mkString("[", ", ", "]")}")
        }
        filteredProviders.head
    }
    // TODO support security connection
    selectedProvider.getConnection(kyuubiConf)
  }

  def loadProviders(): Seq[JdbcConnectionProvider] = {
    val loader = ServiceLoader.load(
      classOf[JdbcConnectionProvider],
      Thread.currentThread().getContextClassLoader)
    val providers = ArrayBuffer[JdbcConnectionProvider]()

    val iterator = loader.iterator()
    while (iterator.hasNext) {
      try {
        val provider = iterator.next()
        info(s"Loaded provider: $provider")
        providers += provider
      } catch {
        case t: Throwable =>
          warn(s"Loaded of the provider failed with the exception", t)
      }
    }

    // TODO support disable provider
    providers
  }
}

object ConnectionProvider extends AbstractConnectionProvider
