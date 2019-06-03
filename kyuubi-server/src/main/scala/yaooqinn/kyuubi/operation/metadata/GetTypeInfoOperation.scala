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
import org.apache.spark.sql.types._

import yaooqinn.kyuubi.operation.{FINISHED, GET_TYPE_INFO, RUNNING}
import yaooqinn.kyuubi.session.KyuubiSession

class GetTypeInfoOperation(session: KyuubiSession)
  extends MetadataOperation(session, GET_TYPE_INFO) {

  /**
   * Implemented by subclasses to decide how to execute specific behavior.
   */
  override protected def runInternal(): Unit = {
    setState(RUNNING)
    // TODO: What a mess here!
    iter = Seq(
      Row("void", java.sql.Types.NULL, null, null, null, null,
        1.toShort, false, 3.toShort, true, false, false,
        null, 0.toShort, 0.toShort, null, null, null),
      Row(BooleanType.typeName, java.sql.Types.BOOLEAN, null, null, null, null,
        1.toShort, false, 3.toShort, true, false, false,
        null, 0.toShort, 0.toShort, null, null, null),
      Row(ByteType.typeName, java.sql.Types.TINYINT, 3, null, null, null,
        1.toShort, false, 3.toShort, false, false, false,
        null, 0.toShort, 0.toShort, null, null, 10),
      Row(ShortType.typeName, java.sql.Types.SMALLINT, 5, null, null, null,
        1.toShort, false, 3.toShort, false, false, false,
        null, 0.toShort, 0.toShort, null, null, 10),
      Row(IntegerType.typeName, java.sql.Types.INTEGER, 10, null, null, null,
        1.toShort, false, 3.toShort, false, false, false,
        null, 0.toShort, 0.toShort, null, null, 10),
      Row(LongType.typeName, java.sql.Types.BIGINT, 19, null, null, null,
        1.toShort, false, 3.toShort, false, false, false,
        null, 0.toShort, 0.toShort, null, null, 10),
      Row(FloatType.typeName, java.sql.Types.FLOAT, 7, null, null, null,
        1.toShort, false, 3.toShort, false, false, false,
        null, 0.toShort, 0.toShort, null, null, 10),
      Row(DoubleType.typeName, java.sql.Types.DOUBLE, 15, null, null, null,
        1.toShort, false, 3.toShort, false, false, false,
        null, 0.toShort, 0.toShort, null, null, 10),
      Row(StringType.typeName, java.sql.Types.VARCHAR, null, null, null, null,
        1.toShort, true, 3.toShort, true, false, false,
        null, 0.toShort, 0.toShort, null, null, null),
      Row("decimal", java.sql.Types.DECIMAL, 38, null, null, null,
        1.toShort, false, 3.toShort, false, false, false,
        null, 0.toShort, 0.toShort, null, null, 10),
      Row(DateType.typeName, java.sql.Types.DATE, null, null, null, null,
        1.toShort, false, 3.toShort, true, false, false,
        null, 0.toShort, 0.toShort, null, null, null),
      Row(TimestampType.typeName, java.sql.Types.TIMESTAMP, null, null, null, null,
        1.toShort, false, 3.toShort, true, false, false,
        null, 0.toShort, 0.toShort, null, null, null),
      Row("array", java.sql.Types.ARRAY, null, null, null, null,
        1.toShort, false, 0.toShort, true, false, false,
        null, 0.toShort, 0.toShort, null, null, null),
      Row("map", java.sql.Types.JAVA_OBJECT, null, null, null, null,
        1.toShort, false, 0.toShort, true, false, false,
        null, 0.toShort, 0.toShort, null, null, null),
      Row("struct", java.sql.Types.STRUCT, null, null, null, null,
        1.toShort, false, 0.toShort, true, false, false,
        null, 0.toShort, 0.toShort, null, null, null)
    ).toList.iterator
    setState(FINISHED)
  }

  /**
   * Get the schema of operation result set.
   */
  override val getResultSetSchema: StructType = {
    new StructType()
      .add("TYPE_NAME", "string", nullable = false, "Type name")
      .add("DATA_TYPE", "int", nullable = false, "SQL data type from java.sql.Types")
      .add("PRECISION", "int", nullable = false, "Maximum precision")
      .add("LITERAL_PREFIX", "string", nullable = true, "Prefix used to quote a literal (may be" +
        " null)")
      .add("LITERAL_SUFFIX", "string", nullable = true, "Suffix used to quote a literal (may be" +
        " null)")
      .add("CREATE_PARAMS", "string", nullable = true, "Parameters used in creating the type (may" +
        " be null)")
      .add("NULLABLE", "smallint", nullable = false, "Can you use NULL for this type")
      .add("CASE_SENSITIVE", "boolean", nullable = false, "Is it case sensitive")
      .add("SEARCHABLE", "smallint", nullable = false, "Can you use \"WHERE\" based on this type")
      .add("UNSIGNED_ATTRIBUTE", "boolean", nullable = false, "Is it unsigned")
      .add("FIXED_PREC_SCALE", "boolean", nullable = false, "Can it be a money value")
      .add("AUTO_INCREMENT", "boolean", nullable = false, "Can it be used for an auto-increment" +
        " value")
      .add("LOCAL_TYPE_NAME", "string", nullable = true, "Localized version of type name (may be" +
        " null)")
      .add("MINIMUM_SCALE", "smallint", nullable = false, "Minimum scale supported")
      .add("MAXIMUM_SCALE", "smallint", nullable = false, "Maximum scale supported")
      .add("SQL_DATA_TYPE", "int", nullable = true, "Unused")
      .add("SQL_DATETIME_SUB", "int", nullable = true, "Unused")
      .add("NUM_PREC_RADIX", "int", nullable = false, "Usually 2 or 10")
  }
}
