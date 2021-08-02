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

package org.apache.kyuubi.engine.spark.events

import java.util.Date

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.spark.SparkSQLEngine
import org.apache.kyuubi.service.ServiceState
import org.apache.kyuubi.service.ServiceState.ServiceState

/**
 *
 * @param applicationId application id a.k.a, the unique id for engine
 * @param applicationName the application name
 * @param owner the application user
 * @param shareLevel the share level for this engine
 * @param connectionUrl the jdbc connection string
 * @param master the master type, yarn, k8s, local etc.
 * @param deployMode client/ cluster
 * @param sparkVersion short version of spark distribution
 * @param webUrl the tracking url of this engine
 * @param driverCores driver cores specified
 * @param driverMemoryMB driver memory specified
 * @param executorCores executor cores specified
 * @param executorMemoryMB driver memory specified
 * @param maxExecutors max number of executors
 * @param startTime start time
 * @param endTime end time
 * @param state the engine state
 * @param diagnostic caught exceptions if any
 */
case class EngineEvent(
    applicationId: String,
    attemptId: Option[String],
    applicationName: String,
    owner: String,
    shareLevel: String,
    connectionUrl: String,
    master: String,
    deployMode: String,
    sparkVersion: String,
    webUrl: String,
    driverCores: Int,
    driverMemoryMB: Int,
    executorCores: Int,
    executorMemoryMB: Int,
    maxExecutors: Int,
    startTime: Long,
    var endTime: Long = -1L,
    var state: Int = 0,
    var diagnostic: String = "") extends KyuubiEvent {

  override def name: String = "engine"

  override def schema: StructType = Encoders.product[EngineEvent].schema

  override def toJson: String = JsonProtocol.productToJson(this)

  override def toString: String = {
    s"""
       |    Spark application name: $applicationName
       |          application ID:  $applicationId
       |          application web UI: $webUrl
       |          master: $master
       |          deploy mode: $deployMode
       |          version: $sparkVersion
       |          driver: [cpu: $driverCores, mem: $driverMemoryMB MB]
       |          executor: [cpu: $executorCores, mem: $executorMemoryMB MB, maxNum: $maxExecutors]
       |    Start time: ${new Date(startTime)}
       |    ${if (endTime != -1L) "End time: " + new Date(endTime) else ""}
       |    User: $owner (shared mode: $shareLevel)
       |    State: ${ServiceState(state)}
       |    ${if (diagnostic.nonEmpty) "Diagnostic: " + diagnostic else ""}""".stripMargin
  }

  def setEndTime(time: Long): this.type = {
    this.endTime = time
    this
  }

  def setDiagnostic(diagnostic: String): this.type = {
    this.diagnostic = diagnostic
    this
  }

  def setState(newState: ServiceState): this.type = {
    this.state = newState.id
    this
  }
}

object EngineEvent {

  def apply(engine: SparkSQLEngine): EngineEvent = {
    val sc = engine.spark.sparkContext
    val webUrl = sc.getConf.getOption(
      "spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES")
      .orElse(sc.uiWebUrl).getOrElse("")
    // need to consider deploy mode and cluster to get core and mem
    val driverCores = sc.getConf.getInt("spark.driver.cores", 0)
    val driverMemory = sc.getConf.getSizeAsMb("spark.driver.memory", "1g").toInt
    val executorCore = sc.getConf.getInt("spark.executor.cores", 1)
    val executorMemory = sc.getConf.getSizeAsMb("spark.executor.memory", "1g").toInt
    val dae = sc.getConf.getBoolean("spark.dynamicAllocation.enabled", defaultValue = false)
    val maxExecutors = if (dae) {
      sc.getConf.getInt("spark.dynamicAllocation.maxExecutors", Int.MaxValue)
    } else {
      sc.getConf.getInt("spark.executor.instances", 1)
    }
    new EngineEvent(
      sc.applicationId,
      sc.applicationAttemptId,
      sc.appName,
      sc.sparkUser,
      engine.getConf.get(ENGINE_SHARE_LEVEL),
      engine.connectionUrl,
      sc.master,
      sc.deployMode,
      sc.version,
      webUrl,
      driverCores,
      driverMemory,
      executorCore,
      executorMemory,
      maxExecutors,
      sc.startTime,
      engine.getServiceState.id)
  }
}
