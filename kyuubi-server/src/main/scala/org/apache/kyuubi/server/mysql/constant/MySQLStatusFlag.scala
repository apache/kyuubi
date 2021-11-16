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

sealed abstract class MySQLStatusFlag(val value: Int)

object MySQLStatusFlag {

  object SERVER_STATUS_IN_TRANS extends MySQLStatusFlag(0x0001)

  object SERVER_STATUS_AUTOCOMMIT extends MySQLStatusFlag(0x0002)

  object SERVER_MORE_RESULTS_EXISTS extends MySQLStatusFlag(0x0008)

  object SERVER_STATUS_NO_GOOD_INDEX_USED extends MySQLStatusFlag(0x0010)

  object SERVER_STATUS_NO_INDEX_USED extends MySQLStatusFlag(0x0020)

  object SERVER_STATUS_CURSOR_EXISTS extends MySQLStatusFlag(0x0040)

  object SERVER_STATUS_LAST_ROW_SENT extends MySQLStatusFlag(0x0080)

  object SERVER_STATUS_DB_DROPPED extends MySQLStatusFlag(0x0100)

  object SERVER_STATUS_NO_BACKSLASH_ESCAPES extends MySQLStatusFlag(0x0200)

  object SERVER_STATUS_METADATA_CHANGED extends MySQLStatusFlag(0x0400)

  object SERVER_QUERY_WAS_SLOW extends MySQLStatusFlag(0x0800)

  object SERVER_PS_OUT_PARAMS extends MySQLStatusFlag(0x1000)

  object SERVER_STATUS_IN_TRANS_READONLY extends MySQLStatusFlag(0x2000)

  object SERVER_SESSION_STATE_CHANGED extends MySQLStatusFlag(0x4000)
}
