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

  /**
   * String type.
   * Catalog name. NULL if not applicable
   */
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

  /**
   * String
   * table type. Typical types are "TABLE", "VIEW", ...
   */
  final val TABLE_TYPE = "TABLE_TYPE"

  /**
   * String => explanatory comment on the table
   */
  final val REMARKS = "REMARKS"

  /**
   * String => column name
   */
  final val COLUMN_NAME = "COLUMN_NAME"

  /**
   * int => SQL type from [[java.sql.Types]]
   */
  final val DATA_TYPE = "DATA_TYPE"

  /**
   * String => Data source dependent type name, for a UDT the type name is fully qualified
   */
  final val TYPE_NAME = "TYPE_NAME"

  /**
   * int => column size
   */
  final val COLUMN_SIZE = "COLUMN_SIZE"

  /**
   * is not used.
   */
  final val BUFFER_LENGTH = "BUFFER_LENGTH"

  /**
   * int => the number of fractional digits.
   * Null is returned for data types where DECIMAL_DIGITS is not applicable.
   */
  final val DECIMAL_DIGITS = "DECIMAL_DIGITS"

  /**
   * int => Radix (typically either 10 or 2)
   */
  final val NUM_PREC_RADIX = "NUM_PREC_RADIX"

  /**
   * Short
   * Can you use NULL for this type?
   */
  final val NULLABLE = "NULLABLE"

  /**
   * String => default value for the column, which should be interpreted as a string when the value
   * is enclosed in single quotes (may be null)
   */
  final val COLUMN_DEF = "COLUMN_DEF"

  /**
   * is not used.
   */
  final val SQL_DATA_TYPE = "SQL_DATA_TYPE"

  /**
   * is not used.
   */
  final val SQL_DATETIME_SUB = "SQL_DATETIME_SUB"

  /**
   * int => for char types the maximum number of bytes in the column
   */
  final val CHAR_OCTET_LENGTH = "CHAR_OCTET_LENGTH"

  /**
   * int => index of column in table (starting at 1)
   */
  final val ORDINAL_POSITION = "ORDINAL_POSITION"
  /**
   * String => ISO rules are used to determine the nullability for a column.
   * - YES --- if the column can include NULLs
   * - NO --- if the column cannot include NULLs
   * - empty string --- if the nullability for the column is unknown
   */
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

  /**
   * String => function catalog (may be null)
   */
  final val FUNCTION_CAT = "FUNCTION_CAT"

  /**
   * String => function schema (may be null)
   */
  final val FUNCTION_SCHEM = "FUNCTION_SCHEM"

  /**
   * String => function name. This is the name used to invoke the function
   */
  final val FUNCTION_NAME = "FUNCTION_NAME"

  /**
   * short => kind of function:
   * - functionResultUnknown - Cannot determine if a return value or table will be returned
   * - functionNoTable- Does not return a table
   * - functionReturnsTable - Returns a table
   */
  final val FUNCTION_TYPE = "FUNCTION_TYPE"

  /**
   * String => the name which uniquely identifies this function within its schema.
   * This is a user specified, or DBMS generated, name that may be different then the FUNCTION_NAME
   * for example with overload functions
   */
  final val SPECIFIC_NAME = "SPECIFIC_NAME"
}
