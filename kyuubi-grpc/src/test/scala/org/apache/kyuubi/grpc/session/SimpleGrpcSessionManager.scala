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
package org.apache.kyuubi.grpc.session

import org.apache.kyuubi.grpc.operation.SimpleGrpcOperationManager

class SimpleGrpcSessionManager
  extends GrpcSessionManager[SimpleGrpcSessionImpl]("simpleTest") {
  override protected def isServer: Boolean = true

  override def grpcOperationManager: SimpleGrpcOperationManager = new SimpleGrpcOperationManager()

  override def getOrCreateSession(key: SessionKey): SimpleGrpcSessionImpl = {
    val session = new SimpleGrpcSessionImpl(key.userId, this)
    session
  }

}
