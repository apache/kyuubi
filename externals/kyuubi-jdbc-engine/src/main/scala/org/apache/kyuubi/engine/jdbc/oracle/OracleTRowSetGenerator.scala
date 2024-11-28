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
package org.apache.kyuubi.engine.jdbc.oracle

import java.sql.Types

import org.apache.kyuubi.engine.jdbc.schema.{Column, DefaultJdbcTRowSetGenerator}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TColumn, TColumnValue}

class OracleTRowSetGenerator extends DefaultJdbcTRowSetGenerator {

  override def toIntegerTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn = {
    asIntegerTColumn(rows, ordinal, (rows, ordinal) => Integer.parseInt(rows(ordinal).toString))
  }

  override def toIntegerTColumnValue(row: Seq[_], ordinal: Int): TColumnValue = {
    asIntegerTColumnValue(row, ordinal, x => Integer.parseInt(x.toString))
    super.toIntegerTColumnValue(row, ordinal)
  }

  override def getColumnType(schema: Seq[Column], ordinal: Int): Int = {
    schema(ordinal).sqlType match {
      case Types.NUMERIC if schema(ordinal).scale == 0 =>
        Types.INTEGER
      case Types.NUMERIC =>
        Types.DECIMAL
      case _ => super.getColumnType(schema, ordinal)
    }
  }
}
