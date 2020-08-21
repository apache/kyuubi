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

package org.apache.kyuubi.engine.spark.operation

import java.util.regex.Pattern

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.operation.OperationType
import org.apache.kyuubi.session.Session

class GetSchemas(spark: SparkSession, session: Session, catalogName: String, schema: String)
  extends SparkOperation(spark, OperationType.GET_SCHEMAS, session) {

  override def statement: String = {
    super.statement + s" [catalog : $catalogName, schemaPattern : $schema]"
  }

  override protected def resultSchema: StructType = {
    new StructType()
      .add("TABLE_SCHEM", "string", nullable = true, "Schema name.")
      .add("TABLE_CATALOG", "string", nullable = true, "Catalog name.")
  }

  override protected def runInternal(): Unit = {
    try {
      val schemaPattern = convertSchemaPattern(schema)
      val databases = spark.sessionState.catalog.listDatabases(schemaPattern)
      val globalTmpViewDb = spark.sessionState.catalog.globalTempViewManager.database
      if (schema == null ||
        Pattern.compile(convertIdentifierPattern(schema, false))
          .matcher(globalTmpViewDb).matches()) {
        iter = (databases :+ globalTmpViewDb).map(Row(_, "")).toList.iterator
      } else {
        iter = databases.map(Row(_, "")).toList.iterator
      }
    } catch onError()
  }
}
