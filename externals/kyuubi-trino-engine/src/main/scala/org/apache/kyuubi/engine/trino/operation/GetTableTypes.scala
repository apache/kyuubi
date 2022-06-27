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

package org.apache.kyuubi.engine.trino.operation

import org.apache.kyuubi.engine.trino.TrinoStatement
import org.apache.kyuubi.operation.ArrayFetchIterator
import org.apache.kyuubi.session.Session

class GetTableTypes(session: Session)
  extends TrinoOperation(session) {

  override protected def runInternal(): Unit = {
    try {
      val trinoStatement = TrinoStatement(
        trinoContext,
        session.sessionManager.getConf,
        "SELECT TABLE_TYPE FROM system.jdbc.table_types")
      schema = trinoStatement.getColumns
      val resultSet = trinoStatement.execute()
      iter = new ArrayFetchIterator(resultSet.toArray)
    } catch onError()
  }
}
