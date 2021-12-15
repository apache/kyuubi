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

sealed abstract class MySQLFieldDetailFlag(val value: Int)

// https://mariadb.com/kb/en/library/resultset/#field-detail-flag
object MySQLFieldDetailFlag {

  object NOT_NULL extends MySQLFieldDetailFlag(0x00000001)

  object PRIMARY_KEY extends MySQLFieldDetailFlag(0x00000002)

  object UNIQUE_KEY extends MySQLFieldDetailFlag(0x00000004)

  object MULTIPLE_KEY extends MySQLFieldDetailFlag(0x00000008)

  object BLOB extends MySQLFieldDetailFlag(0x00000010)

  object UNSIGNED extends MySQLFieldDetailFlag(0x00000020)

  object ZEROFILL_FLAG extends MySQLFieldDetailFlag(0x00000040)

  object BINARY_COLLATION extends MySQLFieldDetailFlag(0x00000080)

  object ENUM extends MySQLFieldDetailFlag(0x00000100)

  object AUTO_INCREMENT extends MySQLFieldDetailFlag(0x00000200)

  object TIMESTAMP extends MySQLFieldDetailFlag(0x00000400)

  object SET extends MySQLFieldDetailFlag(0x00000800)

  object NO_DEFAULT_VALUE_FLAG extends MySQLFieldDetailFlag(0x00001000)

  object ON_UPDATE_NOW_FLAG extends MySQLFieldDetailFlag(0x00002000)

  object NUM_FLAG extends MySQLFieldDetailFlag(0x00008000)
}
