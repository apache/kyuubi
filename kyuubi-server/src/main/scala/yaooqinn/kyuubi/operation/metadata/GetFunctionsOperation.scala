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

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.command.ShowFunctionsCommand
import org.apache.spark.sql.types.StructType

import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.operation.{ERROR, FINISHED, GET_FUNCTIONS, RUNNING}
import yaooqinn.kyuubi.session.KyuubiSession

class GetFunctionsOperation(
    session: KyuubiSession,
    catalogName: String,
    schemaName: String,
    functionName: String) extends MetadataOperation(session, GET_FUNCTIONS) {

  /**
   * Implemented by subclasses to decide how to execute specific behavior.
   */
  override protected def runInternal(): Unit = {
    setState(RUNNING)
    try {
      val command = ShowFunctionsCommand(
        Option(schemaName),
        Option(functionName),
        showUserFunctions = true,
        showSystemFunctions = true)
      val sparkRows = command.run(session.sparkSession)
      iter = sparkRows.map(r => Row(null, null, r.getString(0), null, null, null)).toIterator
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
      .add("FUNCTION_CAT", "string", nullable = true, "Function catalog (may be null)")
      .add("FUNCTION_SCHEM", "string", nullable = true, "Function schema (may be null)")
      .add("FUNCTION_NAME", "string", nullable = true, "Function name. This is the name used to" +
        " invoke the function")
      .add("REMARKS", "string", nullable = true, "Explanatory comment on the function")
      .add("FUNCTION_TYPE", "string", nullable = true, "Kind of function.")
      .add("SPECIFIC_NAME", "string", nullable = true, "The name which uniquely identifies this" +
        " function within its schema")
  }
}
