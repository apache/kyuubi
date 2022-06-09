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
package org.apache.kyuubi.ctl

import scala.collection.JavaConverters._

import org.apache.kyuubi.client.api.v1.dto.{Batch, GetBatchesResponse}
import org.apache.kyuubi.ctl.DateTimeUtil._
import org.apache.kyuubi.ha.client.ServiceNodeInfo

object Render {

  private[ctl] def renderServiceNodesInfo(
      title: String,
      serviceNodeInfo: Seq[ServiceNodeInfo],
      verbose: Boolean): String = {
    val header = Seq("Namespace", "Host", "Port", "Version")
    val rows = serviceNodeInfo.sortBy(_.nodeName).map { sn =>
      Seq(sn.namespace, sn.host, sn.port.toString, sn.version.getOrElse(""))
    }
    Tabulator.format(title, header, rows, verbose)
  }

  private[ctl] def renderBatchListInfo(batchListInfo: GetBatchesResponse): String = {
    val title = s"Total number of batches: ${batchListInfo.getTotal}"
    val header =
      Seq("Id", "Name", "User", "Type", "Instance", "State", "App Info", "Create Time", "End Time")
    val rows = batchListInfo.getBatches.asScala.sortBy(_.getCreateTime).map { batch =>
      Seq(
        batch.getId,
        batch.getName,
        batch.getUser,
        batch.getBatchType,
        batch.getKyuubiInstance,
        batch.getState,
        batch.getBatchInfo.toString,
        millisToDateString(batch.getCreateTime, "yyyy-MM-dd HH:mm:ss"),
        millisToDateString(batch.getEndTime, "yyyy-MM-dd HH:mm:ss"))
    }
    Tabulator.format(title, header, rows, true)
  }

  private[ctl] def renderBatchInfo(batch: Batch): String = {
    s"""Batch Info:
       |  Batch Id: ${batch.getId}
       |  Type: ${batch.getBatchType}
       |  Name: ${batch.getName}
       |  User: ${batch.getUser}
       |  State: ${batch.getState}
       |  Kyuubi Instance: ${batch.getKyuubiInstance}
       |  Create Time: ${millisToDateString(batch.getCreateTime, "yyyy-MM-dd HH:mm:ss")}
       |  End Time: ${millisToDateString(batch.getEndTime, "yyyy-MM-dd HH:mm:ss")}
       |  App Info: ${batch.getBatchInfo.toString}
        """.stripMargin
  }
}
