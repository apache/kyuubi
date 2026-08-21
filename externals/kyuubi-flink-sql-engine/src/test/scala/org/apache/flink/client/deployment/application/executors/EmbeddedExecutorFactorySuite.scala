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

package org.apache.flink.client.deployment.application.executors

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.util.{ArrayList, Collection}

import org.apache.flink.api.common.JobID
import org.apache.flink.runtime.dispatcher.DispatcherGateway
import org.apache.flink.util.concurrent.ScheduledExecutor

import org.apache.kyuubi.KyuubiFunSuite

class EmbeddedExecutorFactorySuite extends KyuubiFunSuite {

  test("reserve Flink application job ids for only one executor") {
    val applicationJobIds = new ArrayList[JobID]()
    new EmbeddedExecutorFactory(
      applicationJobIds,
      proxy(classOf[DispatcherGateway]),
      proxy(classOf[ScheduledExecutor]))

    val bootstrapExecutorJobIds = claimJobIdsForExecutor()
    val statementExecutorJobIds = claimJobIdsForExecutor()

    assert(bootstrapExecutorJobIds eq applicationJobIds)
    assert(statementExecutorJobIds ne applicationJobIds)

    statementExecutorJobIds.add(new JobID())
    assert(applicationJobIds.isEmpty)
  }

  private def claimJobIdsForExecutor(): Collection[JobID] = {
    val claimMethod = classOf[EmbeddedExecutorFactory]
      .getDeclaredMethod("claimJobIdsForExecutor")
    claimMethod.setAccessible(true)
    claimMethod.invoke(null).asInstanceOf[Collection[JobID]]
  }

  private def proxy[T](interfaceClass: Class[T]): T = {
    val invocationHandler = new InvocationHandler {
      override def invoke(proxy: Object, method: Method, args: Array[Object]): Object = null
    }
    Proxy.newProxyInstance(
      interfaceClass.getClassLoader,
      Array(interfaceClass),
      invocationHandler).asInstanceOf[T]
  }
}
