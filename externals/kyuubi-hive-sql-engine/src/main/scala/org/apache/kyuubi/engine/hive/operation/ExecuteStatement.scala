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

package org.apache.kyuubi.engine.hive.operation

import scala.collection.JavaConverters._

import org.apache.hive.service.cli.operation.Operation

import org.apache.kyuubi.engine.hive.event.HiveOperationEvent
import org.apache.kyuubi.events.EventLogging
import org.apache.kyuubi.operation.OperationType
import org.apache.kyuubi.session.Session

class ExecuteStatement(
    session: Session,
    override val statement: String,
    confOverlay: Map[String, String],
    override val shouldRunAsync: Boolean,
    queryTimeout: Long)
  extends HiveOperation(OperationType.EXECUTE_STATEMENT, session) {

  EventLogging.onEvent(HiveOperationEvent(this))

  override val internalHiveOperation: Operation = {
    delegatedOperationManager.newExecuteStatementOperation(
      hive,
      statement,
      confOverlay.asJava,
      shouldRunAsync,
      queryTimeout)
  }
}
