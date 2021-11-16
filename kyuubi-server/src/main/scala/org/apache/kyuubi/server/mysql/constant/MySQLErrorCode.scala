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

package org.apache.kyuubi.server.mysql.constant

import org.apache.kyuubi.KyuubiSQLException

case class MySQLErrorCode(errorCode: Int, sqlState: String, errorMessage: String) {
  override def toString: String = s"ERROR $errorCode ($sqlState): $errorMessage"

  def toKyuubiSQLException: KyuubiSQLException = {
    new KyuubiSQLException(errorMessage, sqlState, errorCode, null)
  }
}

object MySQLErrorCode {

  object TOO_MANY_CONNECTIONS_EXCEPTION extends MySQLErrorCode(
      1040,
      "08004",
      "Too many connections")

  object RUNTIME_EXCEPTION extends MySQLErrorCode(
      1997,
      "C1997",
      "Runtime exception: %s")

  object UNSUPPORTED_COMMAND extends MySQLErrorCode(
      1998,
      "C1998",
      "Unsupported command: %s")

  object UNKNOWN_EXCEPTION extends MySQLErrorCode(
      1999,
      "C1999",
      "Unknown exception: %s")

  object ER_DBACCESS_DENIED_ERROR extends MySQLErrorCode(
      1044,
      "42000",
      "Access denied for user '%s'@'%s' to database '%s'")

  object ER_ACCESS_DENIED_ERROR extends MySQLErrorCode(
      1045,
      "28000",
      "Access denied for user '%s'@'%s' (using password: %s)")

  object ER_NO_DB_ERROR extends MySQLErrorCode(
      1046,
      "3D000",
      "No database selected")

  object ER_BAD_DB_ERROR extends MySQLErrorCode(
      1049,
      "42000",
      "Unknown database '%s'")

  object ER_INTERNAL_ERROR extends MySQLErrorCode(
      1815,
      "HY000",
      "Internal error: %s")

  object ER_UNSUPPORTED_PS extends MySQLErrorCode(
      1295,
      "HY000",
      "This command is not supported in the prepared statement protocol yet")

  object ER_DB_CREATE_EXISTS_ERROR extends MySQLErrorCode(
      1007,
      "HY000",
      "Can't create database '%s'; database exists")

  object ER_DB_DROP_EXISTS_ERROR extends MySQLErrorCode(
      1008,
      "HY000",
      "Can't drop database '%s'; database doesn't exist")

  object ER_TABLE_EXISTS_ERROR extends MySQLErrorCode(
      1050,
      "42S01",
      "Table '%s' already exists")

  object ER_NO_SUCH_TABLE extends MySQLErrorCode(
      1146,
      "42S02",
      "Table '%s' doesn't exist")

  object ER_NOT_SUPPORTED_YET extends MySQLErrorCode(
      1235,
      "42000",
      "This version of Kyuubi-Server doesn't yet support this SQL. '%s'")
}
