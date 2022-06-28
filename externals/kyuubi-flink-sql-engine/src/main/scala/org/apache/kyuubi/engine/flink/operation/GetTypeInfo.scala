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

package org.apache.kyuubi.engine.flink.operation

import java.lang
import java.sql.Types._

import org.apache.flink.table.api.{DataTypes, ResultKind}
import org.apache.flink.table.catalog.Column
import org.apache.flink.table.types.logical.LogicalTypeRoot
import org.apache.flink.types.Row

import org.apache.kyuubi.engine.flink.result.ResultSet
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._
import org.apache.kyuubi.session.Session

class GetTypeInfo(session: Session) extends FlinkOperation(session) {

  private def isNumericType(javaType: Int): Boolean = {
    javaType == TINYINT || javaType == SMALLINT || javaType == INTEGER || javaType == BIGINT ||
    javaType == FLOAT || javaType == DOUBLE || javaType == DECIMAL
  }

  private def toRow(name: String, javaType: Int, precision: Integer = null): Row = {
    Row.of(
      name, // TYPE_NAME
      Integer.valueOf(javaType), // DATA_TYPE
      precision, // PRECISION
      null, // LITERAL_PREFIX
      null, // LITERAL_SUFFIX
      null, // CREATE_PARAMS
      lang.Short.valueOf("1"), // NULLABLE
      lang.Boolean.valueOf(javaType == VARCHAR), // CASE_SENSITIVE
      if (javaType < 1111) lang.Short.valueOf("3") else lang.Short.valueOf("0"), // SEARCHABLE
      lang.Boolean.valueOf(!isNumericType(javaType)), // UNSIGNED_ATTRIBUTE
      lang.Boolean.valueOf(false), // FIXED_PREC_SCALE
      lang.Boolean.valueOf(false), // AUTO_INCREMENT
      null, // LOCAL_TYPE_NAME
      lang.Short.valueOf("1"), // MINIMUM_SCALE
      lang.Short.valueOf("1"), // MAXIMUM_SCALE
      null, // SQL_DATA_TYPE
      null, // SQL_DATETIME_SUB
      if (isNumericType(javaType)) Integer.valueOf(10) else null // NUM_PREC_RADIX
    )
  }

  override protected def runInternal(): Unit = {
    try {
      val dataTypes: Array[Row] = Array(
        toRow(LogicalTypeRoot.CHAR.name(), CHAR),
        toRow(LogicalTypeRoot.VARCHAR.name(), VARCHAR),
        toRow(LogicalTypeRoot.BOOLEAN.name(), BOOLEAN),
        toRow(LogicalTypeRoot.BINARY.name(), BINARY),
        toRow(LogicalTypeRoot.VARBINARY.name(), VARBINARY),
        toRow(LogicalTypeRoot.DECIMAL.name(), DECIMAL, 38),
        toRow(LogicalTypeRoot.TINYINT.name(), TINYINT, 3),
        toRow(LogicalTypeRoot.SMALLINT.name(), SMALLINT, 5),
        toRow(LogicalTypeRoot.INTEGER.name(), INTEGER, 10),
        toRow(LogicalTypeRoot.BIGINT.name(), BIGINT, 19),
        toRow(LogicalTypeRoot.FLOAT.name(), FLOAT, 7),
        toRow(LogicalTypeRoot.DOUBLE.name(), DOUBLE, 15),
        toRow(LogicalTypeRoot.DATE.name(), DATE),
        toRow(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE.name(), TIME),
        toRow(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE.name(), TIMESTAMP),
        toRow(LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE.name(), TIMESTAMP_WITH_TIMEZONE),
        toRow(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE.name(), TIMESTAMP_WITH_TIMEZONE),
        toRow(LogicalTypeRoot.INTERVAL_YEAR_MONTH.name(), OTHER),
        toRow(LogicalTypeRoot.INTERVAL_DAY_TIME.name(), OTHER),
        toRow(LogicalTypeRoot.ARRAY.name(), ARRAY),
        toRow(LogicalTypeRoot.MULTISET.name(), JAVA_OBJECT),
        toRow(LogicalTypeRoot.MAP.name(), JAVA_OBJECT),
        toRow(LogicalTypeRoot.ROW.name(), JAVA_OBJECT),
        toRow(LogicalTypeRoot.DISTINCT_TYPE.name(), OTHER),
        toRow(LogicalTypeRoot.STRUCTURED_TYPE.name(), OTHER),
        toRow(LogicalTypeRoot.NULL.name(), NULL),
        toRow(LogicalTypeRoot.RAW.name(), OTHER),
        toRow(LogicalTypeRoot.SYMBOL.name(), OTHER),
        toRow(LogicalTypeRoot.UNRESOLVED.name(), OTHER))
      resultSet = ResultSet.builder.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
        .columns(
          Column.physical(TYPE_NAME, DataTypes.STRING()),
          Column.physical(DATA_TYPE, DataTypes.INT()),
          Column.physical(PRECISION, DataTypes.INT()),
          Column.physical("LITERAL_PREFIX", DataTypes.STRING()),
          Column.physical("LITERAL_SUFFIX", DataTypes.STRING()),
          Column.physical("CREATE_PARAMS", DataTypes.STRING()),
          Column.physical(NULLABLE, DataTypes.SMALLINT()),
          Column.physical(CASE_SENSITIVE, DataTypes.BOOLEAN()),
          Column.physical(SEARCHABLE, DataTypes.SMALLINT()),
          Column.physical("UNSIGNED_ATTRIBUTE", DataTypes.BOOLEAN()),
          Column.physical("FIXED_PREC_SCALE", DataTypes.BOOLEAN()),
          Column.physical("AUTO_INCREMENT", DataTypes.BOOLEAN()),
          Column.physical("LOCAL_TYPE_NAME", DataTypes.STRING()),
          Column.physical("MINIMUM_SCALE", DataTypes.SMALLINT()),
          Column.physical("MAXIMUM_SCALE", DataTypes.SMALLINT()),
          Column.physical(SQL_DATA_TYPE, DataTypes.INT()),
          Column.physical(SQL_DATETIME_SUB, DataTypes.INT()),
          Column.physical(NUM_PREC_RADIX, DataTypes.INT()))
        .data(dataTypes)
        .build
    } catch {
      onError()
    }
  }
}
