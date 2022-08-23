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
package org.apache.kyuubi.events

import org.apache.kyuubi.{KyuubiException, KyuubiFunSuite}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_EVENT_LOGGERS
import org.apache.kyuubi.events.handler.{CustomEventHandlerProvider, EventHandler, EventHandlerLoader}

class CustomEventHandlerSuite extends KyuubiFunSuite {

  test("load custom event handler") {
    val kyuubiConf = new KyuubiConf()
    kyuubiConf.set(ENGINE_EVENT_LOGGERS.key, "custom")
    val providers = EventHandlerLoader.loadCustom(kyuubiConf)
    assert(providers.head.getClass === classOf[Fake1EventHandler])
    assert(providers(1).getClass === classOf[Fake2EventHandler])
    assert(providers.size == 2)
  }
}

class Fake1EventHandlerProvider extends CustomEventHandlerProvider {
  override def create(kyuubiConf: KyuubiConf): EventHandler[KyuubiEvent] = {
    new Fake1EventHandler(kyuubiConf)
  }
}

class Fake1EventHandler(kyuubiConf: KyuubiConf) extends EventHandler[KyuubiEvent] {

  override def apply(kyuubiEvent: KyuubiEvent): Unit = {}
}

class Fake2EventHandlerProvider extends CustomEventHandlerProvider {
  override def create(kyuubiConf: KyuubiConf): EventHandler[KyuubiEvent] = {
    new Fake2EventHandler(kyuubiConf)
  }
}

class Fake2EventHandler(kyuubiConf: KyuubiConf) extends EventHandler[KyuubiEvent] {

  override def apply(kyuubiEvent: KyuubiEvent): Unit = {}
}

class ExceptionEventHandlerProvider extends CustomEventHandlerProvider {
  override def create(kyuubiConf: KyuubiConf): EventHandler[KyuubiEvent] = {
    throw new KyuubiException("Testing exception.")
  }
}
