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

class GetTypeInfo(session: Session)
  extends TrinoOperation(session) {

  override protected def runInternal(): Unit = {
    try {
      val trinoStatement = TrinoStatement(
        trinoContext,
        session.sessionManager.getConf,
        """
          |SELECT TYPE_NAME, DATA_TYPE, PRECISION, LITERAL_PREFIX, LITERAL_SUFFIX,
          |CREATE_PARAMS, NULLABLE, CASE_SENSITIVE, SEARCHABLE, UNSIGNED_ATTRIBUTE,
          |FIXED_PREC_SCALE, AUTO_INCREMENT, LOCAL_TYPE_NAME, MINIMUM_SCALE,
          |MAXIMUM_SCALE, SQL_DATA_TYPE, SQL_DATETIME_SUB, NUM_PREC_RADIX
          |FROM system.jdbc.types
          |""".stripMargin)
      schema = trinoStatement.getColumns
      val resultSet = trinoStatement.execute()
      iter = new ArrayFetchIterator(resultSet.toArray)
    } catch onError()
  }
}
