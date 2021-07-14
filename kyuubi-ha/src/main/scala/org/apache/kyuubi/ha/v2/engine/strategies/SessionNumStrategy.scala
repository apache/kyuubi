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

package org.apache.kyuubi.ha.v2.engine.strategies

import org.apache.kyuubi.ha.v2.{InstanceProvider, ProviderStrategy}
import org.apache.kyuubi.ha.v2.engine.EngineInstance

class SessionNumStrategy extends ProviderStrategy[EngineInstance] {

  override def getInstance(provider: InstanceProvider[EngineInstance]): Option[EngineInstance] = {
    val instances = provider.getInstances
    if (instances.size == 0) return None
    instances.sortBy(_.sessionNum).headOption
  }

}

object SessionNumStrategy {

  val NAME: String = "session_num"

  def apply(): SessionNumStrategy = new SessionNumStrategy()

}
