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

package org.apache.kyuubi.config

object KyuubiReservedKeys {
  final val KYUUBI_CLIENT_IP_KEY = "kyuubi.client.ipAddress"
  final val KYUUBI_CLIENT_VERSION_KEY = "kyuubi.client.version"
  final val KYUUBI_SERVER_IP_KEY = "kyuubi.server.ipAddress"
  final val KYUUBI_SESSION_USER_KEY = "kyuubi.session.user"
  final val KYUUBI_SESSION_SIGN_PUBLICKEY = "kyuubi.session.sign.publickey"
  final val KYUUBI_SESSION_USER_SIGN = "kyuubi.session.user.sign"
  final val KYUUBI_SESSION_REAL_USER_KEY = "kyuubi.session.real.user"
  final val KYUUBI_SESSION_CONNECTION_URL_KEY = "kyuubi.session.connection.url"
  // default priority is 10, higher priority will be scheduled first
  // when enabled metadata store priority feature
  final val KYUUBI_BATCH_PRIORITY = "kyuubi.batch.priority"
  final val KYUUBI_BATCH_RESOURCE_UPLOADED_KEY = "kyuubi.batch.resource.uploaded"
  final val KYUUBI_STATEMENT_ID_KEY = "kyuubi.statement.id"
  final val KYUUBI_ENGINE_ID = "kyuubi.engine.id"
  final val KYUUBI_ENGINE_NAME = "kyuubi.engine.name"
  final val KYUUBI_ENGINE_URL = "kyuubi.engine.url"
  final val KYUUBI_ENGINE_SUBMIT_TIME_KEY = "kyuubi.engine.submit.time"
  final val KYUUBI_ENGINE_CREDENTIALS_KEY = "kyuubi.engine.credentials"
  final val KYUUBI_SESSION_HANDLE_KEY = "kyuubi.session.handle"
  final val KYUUBI_SESSION_ALIVE_PROBE = "kyuubi.session.alive.probe"
  final val KYUUBI_SESSION_ENGINE_LAUNCH_HANDLE_GUID =
    "kyuubi.session.engine.launch.handle.guid"
  final val KYUUBI_SESSION_ENGINE_LAUNCH_HANDLE_SECRET =
    "kyuubi.session.engine.launch.handle.secret"
  final val KYUUBI_SESSION_ENGINE_LAUNCH_SUPPORT_RESULT =
    "kyuubi.session.engine.launch.support.result"
  final val KYUUBI_OPERATION_SET_CURRENT_CATALOG = "kyuubi.operation.set.current.catalog"
  final val KYUUBI_OPERATION_GET_CURRENT_CATALOG = "kyuubi.operation.get.current.catalog"
  final val KYUUBI_OPERATION_SET_CURRENT_DATABASE = "kyuubi.operation.set.current.database"
  final val KYUUBI_OPERATION_GET_CURRENT_DATABASE = "kyuubi.operation.get.current.database"
  final val KYUUBI_OPERATION_HANDLE_KEY = "kyuubi.operation.handle"
}
