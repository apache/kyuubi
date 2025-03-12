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

import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.config.KyuubiConf.OPERATION_GET_TABLES_IGNORE_TABLE_PROPERTIES
import org.apache.kyuubi.engine.spark.KyuubiSparkUtil.getSessionConf
import org.apache.kyuubi.engine.spark.util.SparkCatalogUtils
import org.apache.kyuubi.operation.IterableFetchIterator
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._
import org.apache.kyuubi.session.Session

class GetTables(
    session: Session,
    catalog: String,
    schema: String,
    tableName: String,
    tableTypes: Set[String])
  extends SparkOperation(session) {

  protected val ignoreTableProperties =
    getSessionConf(OPERATION_GET_TABLES_IGNORE_TABLE_PROPERTIES, spark)

  override def statement: String = {
    super.statement +
      s" [catalog: $catalog," +
      s" schemaPattern: $schema," +
      s" tablePattern: $tableName," +
      s" tableTypes: ${tableTypes.mkString("(", ", ", ")")}]"
  }

  override protected def resultSchema: StructType = {
    new StructType()
      .add(TABLE_CAT, "string", nullable = true, "Catalog name. NULL if not applicable.")
      .add(TABLE_SCHEM, "string", nullable = true, "Schema name.")
      .add(TABLE_NAME, "string", nullable = true, "Table name.")
      .add(TABLE_TYPE, "string", nullable = true, "The table type, e.g. \"TABLE\", \"VIEW\"")
      .add(REMARKS, "string", nullable = true, "Comments about the table.")
      .add(TYPE_CAT, "string", nullable = true, "The types catalog.")
      .add(TYPE_SCHEM, "string", nullable = true, "the types schema (may be null)")
      .add(TYPE_NAME, "string", nullable = true, "Type name.")
      .add(
        SELF_REFERENCING_COL_NAME,
        "string",
        nullable = true,
        "Name of the designated \"identifier\" column of a typed table.")
      .add(
        REF_GENERATION,
        "string",
        nullable = true,
        "Specifies how values in SELF_REFERENCING_COL_NAME are created.")
  }

  override protected def runInternal(): Unit = {
    try {
      val schemaPattern = toJavaRegex(schema)
      val tablePattern = toJavaRegex(tableName)
      val catalogTablesAndViews =
        SparkCatalogUtils.getCatalogTablesOrViews(
          spark,
          catalog,
          schemaPattern,
          tablePattern,
          ignoreTableProperties)
      iter = new IterableFetchIterator(catalogTablesAndViews)
    } catch {
      onError()
    }
  }
}
