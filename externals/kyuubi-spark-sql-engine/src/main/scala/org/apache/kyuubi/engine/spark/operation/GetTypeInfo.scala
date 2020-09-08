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

import java.sql.Types._

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.operation.OperationType
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._
import org.apache.kyuubi.session.Session

class GetTypeInfo(spark: SparkSession, session: Session)
  extends SparkOperation(spark, OperationType.GET_TYPE_INFO, session) {
  override protected def resultSchema: StructType = {
    new StructType()
      .add(TYPE_NAME, "string", nullable = false, "Type name")
      .add(DATA_TYPE, "int", nullable = false, "SQL data type from java.sql.Types")
      .add(PRECISION, "int", nullable = false, "Maximum precision")
      .add("LITERAL_PREFIX", "string", nullable = true, "Prefix used to quote a literal (may be" +
        " null)")
      .add("LITERAL_SUFFIX", "string", nullable = true, "Suffix used to quote a literal (may be" +
        " null)")
      .add("CREATE_PARAMS", "string", nullable = true, "Parameters used in creating the type (may" +
        " be null)")
      .add(NULLABLE, "smallint", nullable = false, "Can you use NULL for this type")
      .add(CASE_SENSITIVE, "boolean", nullable = false, "Is it case sensitive")
      .add(SEARCHABLE, "smallint", nullable = false, "Can you use 'WHERE' based on this type")
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
      .add(NUM_PREC_RADIX, "int", nullable = false, "Usually 2 or 10")
  }

  private def isNumericType(javaType: Int): Boolean = {
    javaType == TINYINT || javaType == SMALLINT || javaType == INTEGER || javaType == BIGINT ||
      javaType == FLOAT || javaType == FLOAT || javaType == DOUBLE || javaType == DECIMAL
  }

  private def toRow(name: String, javaType: Int, precision: Integer = null): Row = {
    Row(name, // TYPE_NAME
      javaType, // DATA_TYPE
      precision, // PRECISION
      null, // LITERAL_PREFIX
      null, // LITERAL_SUFFIX
      null, // CREATE_PARAMS
      1.toShort, // NULLABLE
      javaType == VARCHAR, // CASE_SENSITIVE
      if (javaType < 1111) 3.toShort else 0.toShort, // SEARCHABLE
      !isNumericType(javaType), // UNSIGNED_ATTRIBUTE
      false, // FIXED_PREC_SCALE
      false, // AUTO_INCREMENT
      null, // LOCAL_TYPE_NAME
      0.toShort, // MINIMUM_SCALE
      0.toShort, // MAXIMUM_SCALE
      null, // SQL_DATA_TYPE
      null, // SQL_DATETIME_SUB
      if (isNumericType(javaType)) 10 else null // NUM_PREC_RADIX
    )
  }

  override protected def runInternal(): Unit = {
    iter = Seq(
      toRow("VOID", NULL),
      toRow("BOOLEAN", BOOLEAN),
      toRow("TINYINT", TINYINT, 3),
      toRow("SMALLINT", SMALLINT, 5),
      toRow("INTEGER", INTEGER, 10),
      toRow("BIGINT", BIGINT, 19),
      toRow("FLOAT", FLOAT, 7),
      toRow("DOUBLE", DOUBLE, 15),
      toRow("VARCHAR", VARCHAR),
      toRow("BINARY", BINARY),
      toRow("DECIMAL", DECIMAL, 38),
      toRow("DATE", DATE),
      toRow("TIMESTAMP", TIMESTAMP),
      toRow("ARRAY", ARRAY),
      toRow("MAP", JAVA_OBJECT),
      toRow("STRUCT", STRUCT),
      toRow("INTERVAL", OTHER)
    ).toList.iterator
  }
}
