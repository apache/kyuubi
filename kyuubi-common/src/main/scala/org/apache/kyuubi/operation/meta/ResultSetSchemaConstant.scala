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

package org.apache.kyuubi.operation.meta

object ResultSetSchemaConstant {

  /**
   * String type.
   * Catalog name. NULL if not applicable
   */
  final val TABLE_CAT = "TABLE_CAT"
  final val TABLE_CATALOG = "TABLE_CATALOG"

  /**
   * String.
   * Schema name
   */
  final val TABLE_SCHEM = "TABLE_SCHEM"

  /**
   * String.
   * Table Name
   */
  final val TABLE_NAME = "TABLE_NAME"
  final val TABLE_TYPE = "TABLE_TYPE"

  final val REMARKS = "REMARKS"

  final val COLUMN_NAME = "COLUMN_NAME"
  final val DATA_TYPE = "DATA_TYPE"
  final val TYPE_NAME = "TYPE_NAME"
  final val COLUMN_SIZE = "COLUMN_SIZE"
  final val BUFFER_LENGTH = "BUFFER_LENGTH"
  final val DECIMAL_DIGITS = "DECIMAL_DIGITS"
  final val NUM_PREC_RADIX = "NUM_PREC_RADIX"

  /**
   * Short
   * Can you use NULL for this type?
   */
  final val NULLABLE = "NULLABLE"
  final val COLUMN_DEF = "COLUMN_DEF"
  final val SQL_DATA_TYPE = "SQL_DATA_TYPE"
  final val SQL_DATETIME_SUB = "SQL_DATETIME_SUB"
  final val CHAR_OCTET_LENGTH = "CHAR_OCTET_LENGTH"
  final val ORDINAL_POSITION = "ORDINAL_POSITION"
  final val IS_NULLABLE = "IS_NULLABLE"
  final val SCOPE_CATALOG = "SCOPE_CATALOG"
  final val SCOPE_SCHEMA = "SCOPE_SCHEMA"
  final val SCOPE_TABLE = "SCOPE_TABLE"
  final val SOURCE_DATA_TYPE = "SOURCE_DATA_TYPE"
  final val IS_AUTO_INCREMENT = "IS_AUTO_INCREMENT"

  /**
   * Maximum precision
   */
  final val PRECISION = "PRECISION"

  /**
   * Boolean
   * Is it case sensitive?
   */
  final val CASE_SENSITIVE = "CASE_SENSITIVE"

  /**
   * Short
   * Can you use 'WHERE' based on this type?
   */
  final val SEARCHABLE = "SEARCHABLE"

  final val FUNCTION_CAT = "FUNCTION_CAT"
  final val FUNCTION_SCHEM = "FUNCTION_SCHEM"
  final val FUNCTION_NAME = "FUNCTION_NAME"
  final val FUNCTION_TYPE = "FUNCTION_TYPE"
  final val SPECIFIC_NAME = "SPECIFIC_NAME"
}
