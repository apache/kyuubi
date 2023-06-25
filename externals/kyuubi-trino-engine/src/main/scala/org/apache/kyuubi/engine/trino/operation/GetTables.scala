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

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.engine.trino.TrinoStatement
import org.apache.kyuubi.operation.ArrayFetchIterator
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant.{TABLE_CAT, TABLE_NAME, TABLE_SCHEM, TABLE_TYPE}
import org.apache.kyuubi.session.Session

class GetTables(
    session: Session,
    catalogName: String,
    schemaName: String,
    tableName: String,
    tableTypes: Set[String])
  extends TrinoOperation(session) {

  private val SEARCH_STRING_ESCAPE: String = "\\"

  override protected def runInternal(): Unit = {
    val query = new StringBuilder(
      """
        |SELECT TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS,
        |TYPE_CAT, TYPE_SCHEM, TYPE_NAME,
        |SELF_REFERENCING_COL_NAME, REF_GENERATION
        |FROM system.jdbc.tables
        |""".stripMargin)

    val filters = ArrayBuffer[String]()
    if (StringUtils.isNotEmpty(catalogName)) {
      filters += s"$TABLE_CAT = '$catalogName'"
    }
    if (StringUtils.isNotEmpty(schemaName)) {
      filters += s"$TABLE_SCHEM LIKE '$schemaName' ESCAPE '$SEARCH_STRING_ESCAPE'"
    }
    if (StringUtils.isNotEmpty(tableName)) {
      filters += s"$TABLE_NAME LIKE '$tableName' ESCAPE '$SEARCH_STRING_ESCAPE'"
    }
    if (tableTypes.nonEmpty) {
      filters += s"(${tableTypes.map { tableType => s"$TABLE_TYPE = '$tableType'" }
          .mkString(" OR ")})"
    }

    if (filters.nonEmpty) {
      query.append(" WHERE ")
      query.append(filters.mkString(" AND "))
    }

    try {
      val trinoStatement =
        TrinoStatement(trinoContext, session.sessionManager.getConf, query.toString)
      schema = trinoStatement.getColumns
      val resultSet = trinoStatement.execute()
      iter = new ArrayFetchIterator(resultSet.toArray)
    } catch onError()
  }
}
