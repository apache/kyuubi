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
package org.apache.kyuubi.ctl.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.kyuubi.client.api.v1.dto.{Batch, Engine, GetBatchesResponse}
import org.apache.kyuubi.ctl.util.DateTimeUtils._
import org.apache.kyuubi.ha.client.ServiceNodeInfo

private[ctl] object Render {

  def renderServiceNodesInfo(title: String, serviceNodeInfo: Seq[ServiceNodeInfo]): String = {
    val header = Array("Namespace", "Host", "Port", "Version")
    val rows = serviceNodeInfo.sortBy(_.nodeName).map { sn =>
      Array(sn.namespace, sn.host, sn.port.toString, sn.version.getOrElse(""))
    }.toArray
    Tabulator.format(title, header, rows)
  }

  def renderEngineNodesInfo(engineNodesInfo: Seq[Engine]): String = {
    val title = s"Engine Node List (total ${engineNodesInfo.size})"
    val header = Array("Namespace", "Instance", "Version")
    val rows = engineNodesInfo.map { engine =>
      Array(engine.getNamespace, engine.getInstance, engine.getVersion)
    }.toArray
    Tabulator.format(title, header, rows)
  }

  def renderBatchListInfo(batchListInfo: GetBatchesResponse): String = {
    val title = s"Batch List (from ${batchListInfo.getFrom} total ${batchListInfo.getTotal})"
    val rows = batchListInfo.getBatches.asScala.sortBy(_.getCreateTime).map(buildBatchRow).toArray
    Tabulator.format(title, batchColumnNames, rows)
  }

  def renderBatchInfo(batch: Batch): String = {
    val title = s"Batch Report (${batch.getId})"
    val header = Array("Key", "Value")
    val rows = batchColumnNames.zip(buildBatchRow(batch)).map { case (k, v) =>
      Array(k, v)
    }
    Tabulator.format(title, header, rows)
  }

  private val batchColumnNames =
    Array(
      "Batch Id",
      "Type",
      "Name",
      "User",
      "State",
      "Batch App Info",
      "Kyuubi Instance",
      "Time Range")

  private def buildBatchRow(batch: Batch): Array[String] = {
    Array(
      batch.getId,
      batch.getBatchType,
      batch.getName,
      batch.getUser,
      batch.getState,
      buildBatchAppInfo(batch).mkString("\n"),
      batch.getKyuubiInstance,
      Seq(
        millisToDateString(batch.getCreateTime, "yyyy-MM-dd HH:mm:ss"),
        millisToDateString(batch.getEndTime, "yyyy-MM-dd HH:mm:ss")).mkString("\n~\n"))
  }

  private def buildBatchAppInfo(batch: Batch): List[String] = {
    val batchAppInfo = ListBuffer[String]()
    Option(batch.getAppId).foreach { _ =>
      batchAppInfo += s"App Id: ${batch.getAppId}"
    }
    Option(batch.getAppUrl).foreach { _ =>
      batchAppInfo += s"App Url: ${batch.getAppUrl}"
    }
    Option(batch.getAppState).foreach { _ =>
      batchAppInfo += s"App State: ${batch.getAppState}"
    }
    Option(batch.getAppDiagnostic).filter(_.nonEmpty).foreach { _ =>
      batchAppInfo += s"App Diagnostic: ${batch.getAppDiagnostic}"
    }
    batchAppInfo.toList
  }
}
