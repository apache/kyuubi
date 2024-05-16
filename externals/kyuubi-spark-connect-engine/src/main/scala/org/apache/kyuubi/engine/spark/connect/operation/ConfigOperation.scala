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
package org.apache.kyuubi.engine.spark.connect.operation

import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter
import io.grpc.stub.StreamObserver
import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.engine.spark.connect.session.SparkConnectSessionImpl
import org.apache.spark.sql.RuntimeConfig
import org.apache.spark.sql.internal.SQLConf
import org.apache.kyuubi.grpc.session.{GrpcSession, SessionKey}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session
import org.apache.spark.connect.proto.{ConfigRequest, ConfigResponse, KeyValue}

class ConfigOperation(
                       session: SparkConnectSessionImpl,
                       request: ConfigRequest,
                       responseObserver: StreamObserver[ConfigResponse])
  extends SparkConnectOperation(session) {
  override protected def runInternal(): Unit = {
    val builder = request.getOperation.getOpTypeCase match {
      case ConfigRequest.Operation.OpTypeCase.SET =>
        handleSet(request.getOperation.getSet, spark.conf)
      case ConfigRequest.Operation.OpTypeCase.GET =>
        handleGet(request.getOperation.getGet, spark.conf)
      case ConfigRequest.Operation.OpTypeCase.GET_WITH_DEFAULT =>
        handleGetWithDefault(request.getOperation.getGetWithDefault, spark.conf)
      case ConfigRequest.Operation.OpTypeCase.GET_OPTION =>
        handleGetOption(request.getOperation.getGetOption, spark.conf)
      case ConfigRequest.Operation.OpTypeCase.GET_ALL =>
        handleGetAll(request.getOperation.getGetAll, spark.conf)
      case ConfigRequest.Operation.OpTypeCase.UNSET =>
        handleUnset(request.getOperation.getUnset, spark.conf)
      case ConfigRequest.Operation.OpTypeCase.IS_MODIFIABLE =>
        handleIsModifiable(request.getOperation.getIsModifiable, spark.conf)
      case _ => throw KyuubiSQLException(s"${request.getOperation} not supported")
    }
    builder.setSessionId(request.getSessionId)
    responseObserver.onNext(builder.build())
    responseObserver.onCompleted()
  }

  private def handleSet(operation: ConfigRequest.Set,
    conf: RuntimeConfig): ConfigResponse.Builder = {
    val builder = ConfigResponse.newBuilder()
    operation.getPairsList.asScala.iterator.foreach { pair =>
      val (key, value) = ConfigOperation.toKeyValue(pair)
      conf.set(key, value.orNull)
      getWarning(key).foreach(builder.addWarnings)
    }
    builder
  }

  private def handleGet(operation: ConfigRequest.Get,
                        conf: RuntimeConfig): ConfigResponse.Builder = {
    val builder = ConfigResponse.newBuilder()
    operation.getKeysList.asScala.iterator.foreach{ key =>
      val value = conf.get(key)
      builder.addPairs(ConfigOperation.toProtoKeyValue(key, Option(value)))
      getWarning(key).foreach(builder.addWarnings)
    }
    builder
  }

  def handleGetWithDefault(operation: ConfigRequest.GetWithDefault,
                           conf: RuntimeConfig): ConfigResponse.Builder = {
    val builder = ConfigResponse.newBuilder()
    operation.getPairsList.asScala.iterator.foreach{ pair =>
      val (key, default) = ConfigOperation.toKeyValue(pair)
      val value = conf.get(key, default.orNull)
      builder.addPairs(ConfigOperation.toProtoKeyValue(key, Option(value)))
      getWarning(key).foreach(builder.addWarnings)
    }
    builder
  }

  private def handleGetOption(operation: ConfigRequest.GetOption,
                              conf: RuntimeConfig): ConfigResponse.Builder = {
    val builder = ConfigResponse.newBuilder()
    operation.getKeysList.asScala.iterator.foreach{ key =>
      val value = conf.getOption(key)
      builder.addPairs(ConfigOperation.toProtoKeyValue(key, value))
      getWarning(key).foreach(builder.addWarnings)
    }
    builder
  }

  private def handleGetAll(operation: ConfigRequest.GetAll,
                           conf: RuntimeConfig): ConfigResponse.Builder = {
    val builder = ConfigResponse.newBuilder()
    val results = if (operation.hasPrefix) {
      val prefix = operation.getPrefix
      conf.getAll.iterator
        .filter{ case (key, _) => key.startsWith(prefix)}
        .map{ case (key, value) => (key.substring(prefix.length), value)}
    } else {
      conf.getAll.iterator
    }
    results.foreach{ case (key, value) =>
      builder.addPairs(ConfigOperation.toProtoKeyValue(key, Option(value)))
      getWarning(key).foreach(builder.addWarnings)
    }
    builder
  }

  private def handleUnset(operation: ConfigRequest.Unset,
                          conf: RuntimeConfig): ConfigResponse.Builder = {
    val builder = ConfigResponse.newBuilder()
    operation.getKeysList.asScala.iterator.foreach{ key =>
      conf.unset(key)
      getWarning(key).foreach(builder.addWarnings)
    }
    builder
  }

  private def handleIsModifiable(operation: ConfigRequest.IsModifiable,
                                 conf: RuntimeConfig): ConfigResponse.Builder = {
    val builder = ConfigResponse.newBuilder()
    operation.getKeysList.asScala.iterator.foreach { key =>
      val value = conf.isModifiable(key)
      builder.addPairs(ConfigOperation.toProtoKeyValue(key, Option(value.toString)))
      getWarning(key).foreach(builder.addWarnings)
    }
    builder
  }

  private def getWarning(key: String): Option[String] = {
    if (ConfigOperation.unsupportedConfigurations.contains(key)) {
      Some(s"The SQL config '$key' is NOT supported in Spark Connect'")
    } else {
      SQLConf.deprecatedSQLConfigs.get(key).map(f => s"The Key '$f' is deprecated")
    }
  }

  override def getOperationLog: Option[OperationLog] = {
    None
  }

  override def isTimedOut: Boolean = {
    false
  }
}

object ConfigOperation {
  private val unsupportedConfigurations = Set("spark.sql.execution.arrow.enabled")

  def toKeyValue(pair: KeyValue): (String, Option[String]) = {
    val key = pair.getKey
    val value = if (pair.hasValue) {
      Some(pair.getValue)
    } else {
      None
    }
    (key, value)
  }

  def toProtoKeyValue(key: String, value: Option[String]): KeyValue = {
    val builder = KeyValue.newBuilder()
    builder.setKey(key)
    value.foreach(builder.setValue)
    builder.build()
  }
}
