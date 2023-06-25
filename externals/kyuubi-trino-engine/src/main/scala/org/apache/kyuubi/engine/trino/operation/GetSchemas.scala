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
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant.{TABLE_CATALOG, TABLE_SCHEM}
import org.apache.kyuubi.session.Session

class GetSchemas(session: Session, catalogName: String, schemaPattern: String)
  extends TrinoOperation(session) {

  private val SEARCH_STRING_ESCAPE: String = "\\"

  override protected def runInternal(): Unit = {
    val query = new StringBuilder("SELECT TABLE_SCHEM, TABLE_CATALOG FROM system.jdbc.schemas")

    val filters = ArrayBuffer[String]()
    if (StringUtils.isNotEmpty(catalogName)) {
      filters += s"$TABLE_CATALOG = '$catalogName'"
    }
    if (StringUtils.isNotEmpty(schemaPattern)) {
      filters += s"$TABLE_SCHEM LIKE '$schemaPattern' ESCAPE '$SEARCH_STRING_ESCAPE'"
    }

    if (filters.nonEmpty) {
      query.append(" WHERE ")
      query.append(filters.mkString(" AND "))
    }

    try {
      val trinoStatement = TrinoStatement(
        trinoContext,
        session.sessionManager.getConf,
        query.toString)
      schema = trinoStatement.getColumns
      val resultSet = trinoStatement.execute()
      iter = new ArrayFetchIterator(resultSet.toArray)
    } catch onError()
  }
}
