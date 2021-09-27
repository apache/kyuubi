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

package org.apache.kyuubi.session

import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.{NoopOperationManager, OperationManager}

class NoopSessionManager extends SessionManager("noop") {
  override val operationManager: OperationManager = new NoopOperationManager()

  override def initialize(conf: KyuubiConf): Unit = {
    _operationLogRoot = Some("target/operation_logs")
    super.initialize(conf)
  }

  override def openSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String]): SessionHandle = {
    if (conf.get("kyuubi.test.should.fail").exists(_.toBoolean)) {
      throw KyuubiSQLException("Asked to fail")
    }
    val session = new NoopSessionImpl(protocol, user, password, ipAddress, conf, this)
    setSession(session.handle, session)
    session.handle
  }

  override protected def isServer: Boolean = true
}
