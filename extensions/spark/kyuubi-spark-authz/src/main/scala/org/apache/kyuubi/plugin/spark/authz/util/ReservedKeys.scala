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

package org.apache.kyuubi.plugin.spark.authz.util

object ReservedKeys {
  final val KYUUBI_SESSION_USER_SIGN_ENABLED = "kyuubi.session.user.sign.enabled"
  final val KYUUBI_SESSION_USER = "kyuubi.session.user"
  final val KYUUBI_SESSION_SIGN_PUBLICKEY = "kyuubi.session.sign.publickey"
  final val KYUUBI_SESSION_USER_SIGN = "kyuubi.session.user.sign"
  final var KYUUBI_EXPLAIN_COMMAND_EXECUTION_ID = "kyuubi.authz.command.explain.executionid"
}
