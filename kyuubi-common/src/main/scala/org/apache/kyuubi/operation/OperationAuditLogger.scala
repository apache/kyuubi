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

import org.apache.kyuubi.Logging
import org.apache.kyuubi.operation.OperationState.OperationState

object OperationAuditLogger extends Logging {
  final private val AUDIT_BUFFER = new ThreadLocal[StringBuilder]() {
    override protected def initialValue: StringBuilder = new StringBuilder()
  }

  def audit(operation: Operation, state: OperationState): Unit = {
    val sb = AUDIT_BUFFER.get()
    sb.setLength(0)
    sb.append(s"operation=${operation.getHandle.identifier}").append("\t")
    sb.append(s"opType=${operation.getClass.getSimpleName}").append("\t")
    sb.append(s"state=$state").append("\t")
    sb.append(s"user=${operation.getSession.user}").append("\t")
    sb.append(s"session=${operation.getSession.handle.identifier}")
    info(sb.toString())
  }
}
