/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.operation.metadata

import org.apache.spark.sql.execution.command.KyuubiShowTablesCommand
import org.apache.spark.sql.types.StructType

import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.operation.{ERROR, FINISHED, GET_TABLES, RUNNING}
import yaooqinn.kyuubi.session.KyuubiSession

class GetTablesOperation (
    session: KyuubiSession,
    catalogName: String,
    schemaName: String,
    tableName: String,
    tableTypes: Seq[String]) extends MetadataOperation(session, GET_TABLES) {

  /**
   * Implemented by subclasses to decide how to execute specific behavior.
   */
  override protected def runInternal(): Unit = {
    setState(RUNNING)
    try {
      val cmd = KyuubiShowTablesCommand(
        convertSchemaPattern(schemaName),
        convertIdentifierPattern(tableName, datanucleusFormat = true),
        tableTypes)
      iter = cmd.run(session.sparkSession).toList.iterator
      setState(FINISHED)
    } catch {
      case e: Exception =>
        setState(ERROR)
        throw new KyuubiSQLException(e)
    }
  }

  /**
   * Get the schema of operation result set.
   */
  override val getResultSetSchema: StructType = {
    new StructType()
      .add("TABLE_CAT", "string", nullable = true, "Catalog name. NULL if not applicable.")
      .add("TABLE_SCHEM", "string", nullable = true, "Schema name.")
      .add("TABLE_NAME", "string", nullable = true, "Table name.")
      .add("TABLE_TYPE", "string", nullable = true, "The table type, e.g. \"TABLE\", \"VIEW\"")
      .add("REMARKS", "string", nullable = true, "Comments about the table.")
  }

}
