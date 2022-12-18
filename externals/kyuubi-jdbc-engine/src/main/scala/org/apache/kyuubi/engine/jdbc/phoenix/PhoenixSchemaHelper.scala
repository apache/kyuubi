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
package org.apache.kyuubi.engine.jdbc.phoenix

import java.sql.Types

import org.apache.hive.service.rpc.thrift._

import org.apache.kyuubi.engine.jdbc.schema.SchemaHelper

class PhoenixSchemaHelper extends SchemaHelper {

  override def toTTypeId(sqlType: Int): TTypeId = sqlType match {
    case Types.BIT =>
      TTypeId.BOOLEAN_TYPE

    case Types.TINYINT =>
      TTypeId.TINYINT_TYPE

    case Types.SMALLINT =>
      TTypeId.SMALLINT_TYPE

    case Types.INTEGER =>
      TTypeId.INT_TYPE

    case Types.BIGINT =>
      TTypeId.BIGINT_TYPE

    case Types.REAL =>
      TTypeId.FLOAT_TYPE

    case Types.DOUBLE =>
      TTypeId.DOUBLE_TYPE

    case Types.CHAR | Types.VARCHAR =>
      TTypeId.STRING_TYPE

    case Types.DATE =>
      TTypeId.DATE_TYPE

    case Types.TIMESTAMP =>
      TTypeId.TIMESTAMP_TYPE

    case Types.DECIMAL =>
      TTypeId.DECIMAL_TYPE

    // TODO add more type support
    case _ =>
      TTypeId.STRING_TYPE
  }
}
