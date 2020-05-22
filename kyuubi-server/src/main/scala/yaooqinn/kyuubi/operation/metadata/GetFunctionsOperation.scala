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

import org.apache.spark.sql.execution.command.KyuubiShowFunctionsCommand
import org.apache.spark.sql.types.StructType

import yaooqinn.kyuubi.operation.GET_FUNCTIONS
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
    execute {
      val f = if (functionName == null) {
        ".*"
      } else {
        var escape = false
        functionName.flatMap {
          case c if escape =>
            if (c != '\\') escape = false
            c.toString
          case '\\' =>
            escape = true
            ""
          case '%' => ".*"
          case '_' => "."
          case c => Character.toLowerCase(c).toString
        }
      }
      val command = KyuubiShowFunctionsCommand(convertSchemaPattern(schemaName), f)
      val sparkRows = command.run(session.sparkSession)
      iter = sparkRows.toList.iterator
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
      .add("FUNCTION_TYPE", "int", nullable = true, "Kind of function.")
      .add("SPECIFIC_NAME", "string", nullable = true, "The name which uniquely identifies this" +
        " function within its schema")
  }
}
