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

import java.sql.DatabaseMetaData

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.engine.spark.IterableFetchIterator
import org.apache.kyuubi.operation.OperationType
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._
import org.apache.kyuubi.session.Session

class GetFunctions(
    spark: SparkSession,
    session: Session,
    catalogName: String,
    schemaName: String,
    functionName: String)
  extends SparkOperation(spark, OperationType.GET_FUNCTIONS, session) {

  override def statement: String = {
    super.statement +
      s" [catalog: $catalogName," +
      s" schemaPattern: $schemaName," +
      s" functionPattern: $functionName]"
  }

  override protected def resultSchema: StructType = {
    new StructType()
      .add(FUNCTION_CAT, "string", nullable = true, "Function catalog (may be null)")
      .add(FUNCTION_SCHEM, "string", nullable = true, "Function schema (may be null)")
      .add(FUNCTION_NAME, "string", nullable = true, "Function name. This is the name used to" +
        " invoke the function")
      .add(REMARKS, "string", nullable = true, "Explanatory comment on the function")
      .add(FUNCTION_TYPE, "int", nullable = true, "Kind of function.")
      .add(SPECIFIC_NAME, "string", nullable = true, "The name which uniquely identifies this" +
        " function within its schema")
  }

  override protected def runInternal(): Unit = {
    try {
      val schemaPattern = toJavaRegex(schemaName)
      val functionPattern = toJavaRegex(functionName)
      val catalog = spark.sessionState.catalog
      val a: Seq[Row] = catalog.listDatabases(schemaPattern).flatMap { db =>
        catalog.listFunctions(db, functionPattern).map { case (f, _) =>
          val info = catalog.lookupFunctionInfo(f)
          Row(
            "",
            info.getDb,
            info.getName,
            s"Usage: ${info.getUsage}\nExtended Usage:${info.getExtended}",
            DatabaseMetaData.functionResultUnknown,
            info.getClassName)
        }
      }
      iter = new IterableFetchIterator(a.toList)
    } catch {
      onError()
    }
  }
}
