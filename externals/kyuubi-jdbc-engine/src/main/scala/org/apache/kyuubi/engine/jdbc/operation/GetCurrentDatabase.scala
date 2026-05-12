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
package org.apache.kyuubi.engine.jdbc.operation

import java.sql.Types

import org.apache.kyuubi.engine.jdbc.schema.{Column, Row, Schema}
import org.apache.kyuubi.engine.jdbc.session.JdbcSessionImpl
import org.apache.kyuubi.operation.{ArrayFetchIterator, OperationState}
import org.apache.kyuubi.session.Session

class GetCurrentDatabase(session: Session) extends JdbcOperation(session) {

  override protected def runInternal(): Unit = {
    setState(OperationState.RUNNING)
    try {
      val connection = session.asInstanceOf[JdbcSessionImpl].sessionConnection
      val database = dialect.getCurrentSchema(connection)
      schema = Schema(List(Column(
        "TABLE_SCHEM",
        "VARCHAR",
        Types.VARCHAR,
        precision = 128,
        scale = 0,
        label = "TABLE_SCHEM",
        displaySize = 128)))
      iter = new ArrayFetchIterator(Array(Row(List(database))))
      setState(OperationState.FINISHED)
    } catch onError()
  }
}
