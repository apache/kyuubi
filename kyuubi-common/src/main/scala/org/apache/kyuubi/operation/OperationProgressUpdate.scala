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

package org.apache.kyuubi.operation

import java.util

import org.apache.commons.lang3.StringUtils
import org.apache.hive.service.rpc.thrift.TJobExecutionStatus

case class OperationProgressUpdate(
    headers: util.List[String],
    rows: util.List[util.List[String]],
    footerSummary: String,
    progressedPercentage: Double,
    startTimeMillis: Long,
    status: String)

object KyuubiProgressMonitorStatusMapper {

  def forStatus(status: String): TJobExecutionStatus = {
    if (StringUtils.isEmpty(status)) {
      return TJobExecutionStatus.NOT_AVAILABLE
    }
    OperationProgressStatus.withName(status) match {
      case OperationProgressStatus.NOT_AVAILABLE =>
        TJobExecutionStatus.NOT_AVAILABLE
      case OperationProgressStatus.IN_PROGRESS | OperationProgressStatus.PENDING
          | OperationProgressStatus.RUNNING =>
        TJobExecutionStatus.IN_PROGRESS
      case OperationProgressStatus.COMPLETE => TJobExecutionStatus.COMPLETE
      case _ => TJobExecutionStatus.COMPLETE
    }
  }
}

object OperationProgressStatus extends Enumeration {
  type OperationProgressStatus = Value
  val PENDING, RUNNING, FINISHED, IN_PROGRESS, COMPLETE, NOT_AVAILABLE = Value
}
