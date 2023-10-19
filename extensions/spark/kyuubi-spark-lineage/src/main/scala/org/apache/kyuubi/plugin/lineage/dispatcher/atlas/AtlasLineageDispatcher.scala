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

package org.apache.kyuubi.plugin.lineage.dispatcher.atlas

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.QueryExecution

import org.apache.kyuubi.plugin.lineage.{Lineage, LineageDispatcher}
import org.apache.kyuubi.plugin.lineage.dispatcher.atlas.AtlasEntityHelper.columnLineageEnabled

class AtlasLineageDispatcher extends LineageDispatcher with Logging {

  override def send(qe: QueryExecution, lineageOpt: Option[Lineage]): Unit = {
    try {
      lineageOpt.filter(l => l.inputTables.nonEmpty || l.outputTables.nonEmpty).foreach(lineage => {
        val processEntity = AtlasEntityHelper.processEntity(qe, lineage)
        val columnLineageEntities = if (lineage.columnLineage.nonEmpty && columnLineageEnabled) {
          AtlasEntityHelper.columnLineageEntities(processEntity, lineage)
        } else {
          Seq.empty
        }
        AtlasClient.getClient().send(processEntity +: columnLineageEntities)
      })
    } catch {
      case t: Throwable =>
        logWarning("Send lineage to atlas failed.", t)
    }
  }

  override def onFailure(qe: QueryExecution, exception: Exception): Unit = {
    // ignore
  }

}
