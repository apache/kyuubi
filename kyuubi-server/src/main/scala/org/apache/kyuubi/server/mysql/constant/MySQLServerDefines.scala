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

import org.apache.kyuubi._

object MySQLServerDefines {
  val PROTOCOL_VERSION = 0x0A
  val CHARSET = 0x2d // utf8mb4_general_ci
  val MYSQL_VERSION = "5.7.22"
  val MYSQL_KYUUBI_SERVER_VERSION = s"$MYSQL_VERSION-Kyuubi-Server $KYUUBI_VERSION"
  val KYUUBI_SERVER_DESCRIPTION = s"Apache Kyuubi (Incubating) v$KYUUBI_VERSION revision $REVISION"
}
