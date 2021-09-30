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

import io.netty.util.AttributeKey
import io.netty.util.AttributeKey._

import org.apache.kyuubi.session.SessionHandle

object MySQLCtxAttrKey {
  val CONNECTION_ID: AttributeKey[Integer] = valueOf("CONNECTION_ID")
  val CAPABILITY_FLAG: AttributeKey[Integer] = valueOf("CAPABILITY_FLAG")
  val USER: AttributeKey[String] = valueOf[String]("USER")
  val REMOTE_IP: AttributeKey[String] = valueOf[String]("REMOTE_IP")
  val DATABASE: AttributeKey[String] = valueOf[String]("DATABASE")
  val SESSION_HANDLE: AttributeKey[SessionHandle] = valueOf[SessionHandle]("SESSION_HANDLE")
}
