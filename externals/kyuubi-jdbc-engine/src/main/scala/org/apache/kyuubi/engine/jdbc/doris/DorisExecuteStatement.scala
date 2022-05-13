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
package org.apache.kyuubi.engine.jdbc.doris

import org.apache.hive.service.rpc.thrift.{TRowSet, TTableSchema}

import org.apache.kyuubi.engine.jdbc.JdbcStatement
import org.apache.kyuubi.engine.jdbc.operation.ExecuteStatement
import org.apache.kyuubi.engine.jdbc.schema.Row
import org.apache.kyuubi.session.Session

class DorisExecuteStatement(
    session: Session,
    override val statement: String,
    override val shouldRunAsync: Boolean,
    queryTimeout: Long,
    incrementalCollect: Boolean)
  extends ExecuteStatement(
    session,
    statement,
    shouldRunAsync,
    queryTimeout,
    incrementalCollect) {

  override protected def toTRowSet(taken: Iterator[Row]): TRowSet = {
    val rowSetHelper = new DorisRowSetHelper()
    rowSetHelper.toTRowSet(
      taken.toList.map(_.values),
      schema.columns,
      getProtocolVersion)
  }

  override def getResultSetSchema: TTableSchema = {
    val schemaHelper = new DorisSchemaHelper
    schemaHelper.toTTTableSchema(schema.columns)
  }

  override protected def getJdbcStatement(): JdbcStatement = {
    new DorisStatement
  }
}
