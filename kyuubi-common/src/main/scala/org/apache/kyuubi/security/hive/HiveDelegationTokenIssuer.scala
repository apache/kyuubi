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

package org.apache.kyuubi.security.hive

import java.net.URI
import java.util.concurrent.TimeUnit

import scala.util.Random

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.metastore.api.{MetaException, ThriftHiveMetastore}
import org.apache.hadoop.security.SaslPropertiesResolver
import org.apache.hadoop.util.StringUtils
import org.apache.thrift.protocol.{TBinaryProtocol, TCompactProtocol}
import org.apache.thrift.transport.{TSocket, TTransport}

import org.apache.kyuubi.Logging

class HiveDelegationTokenIssuer(conf: HiveConf) extends Logging {

  private var client: ThriftHiveMetastore.Iface = _
  private var transport: TTransport = _

  private val metastoreUris: Seq[URI] = {
    val metastoreUrisStr = conf.getVar(ConfVars.METASTOREURIS)
    require(metastoreUrisStr.nonEmpty, "MetaStoreURIs not found in conf file")

    val uris = conf
      .getVar(ConfVars.METASTOREURIS)
      .split(",")
      .map { s =>
        val uri = new URI(s)
        if (uri.getScheme == null) {
          throw new IllegalArgumentException("URI: " + s + " does not have a scheme")
        }
        uri
      }
    Random.shuffle(uris.toSeq)
  }

  private val useSasl: Boolean = conf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL)
  if (!useSasl) {
    warn(s"Delegation token not supported when ${ConfVars.METASTORE_USE_THRIFT_SASL} is false")
  }

  open()

  private def open(): Unit = {
    if (!useSasl) {
      return
    }

    val useCompactProtocol = conf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_COMPACT_PROTOCOL)
    val clientSocketTimeout =
      conf.getTimeVar(ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS).toInt

    var exception: Throwable = null
    val connected = metastoreUris.exists { store =>
      info("Trying to connect to metastore with URI " + store)

      try {
        transport =
          // Wrap thrift connection with SASL for secure connection.
          createSASLTransport(store, clientSocketTimeout)
      } catch {
        case e: Throwable =>
          exception = e
          error("Couldn't create client transport", e)
      }

      var succeeded = false
      if (transport != null) {
        val protocol =
          if (useCompactProtocol) {
            new TCompactProtocol(transport)
          } else {
            new TBinaryProtocol(transport)
          }
        client = new ThriftHiveMetastore.Client(protocol)
        try {
          if (!transport.isOpen) {
            transport.open()
          }
          succeeded = true
        } catch {
          case e: Throwable =>
            exception = e
            warn("Failed to connect to the MetaStore Server...", e)
        }
      }
      succeeded
    }

    if (!connected) {
      throw new MetaException(
        "Could not connect to meta store using any of the URIs provided." +
          " Most recent failure: " + StringUtils.stringifyException(exception))
    }
  }

  private def createSASLTransport(store: URI, clientSocketTimeout: Int): TTransport = {
    val authBridge: HadoopThriftAuthBridgeClient = new HadoopThriftAuthBridgeClient()
    val tSocket = new TSocket(store.getHost, store.getPort, clientSocketTimeout)

    val saslProperties = SaslPropertiesResolver.getInstance(conf).getDefaultProperties
    val principalConfig = conf.getVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL)
    authBridge.createClientTransport(
      principalConfig,
      store.getHost,
      tSocket,
      saslProperties)
  }

  def getDelegationToken(owner: String, renewer: String): String = {
    if (!useSasl) {
      return null
    }
    client.get_delegation_token(owner, renewer)
  }

}
