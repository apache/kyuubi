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

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf

class EventBusSuite extends KyuubiFunSuite {

  case class Test0KyuubiEvent(content: String) extends KyuubiEvent {
    override def partitions: Seq[(String, String)] = Seq[(String, String)]()
  }

  case class Test1KyuubiEvent(content: String) extends KyuubiEvent {
    override def partitions: Seq[(String, String)] = Seq[(String, String)]()
  }


  test("register event handler") {
    val liveBus = EventBus()
    liveBus.register[Test0KyuubiEvent] { e =>
      assert(e.content == "test0")
    }
    liveBus.register[Test1KyuubiEvent] { e =>
      assert(e.content == "test1")
    }

    class Test0Handler extends EventHandler[Test0KyuubiEvent] {
      override def apply(e: Test0KyuubiEvent): Unit = {
        assert(e.content == "test0")
      }
    }

    liveBus.register[Test0KyuubiEvent](new Test0Handler)

    liveBus.register[Test1KyuubiEvent](new EventHandler[Test1KyuubiEvent] {
      override def apply(e: Test1KyuubiEvent): Unit = {
        assert(e.content == "test1")
      }
    })

    (1 to 100) foreach { _ =>
      liveBus.post(Test0KyuubiEvent("test0"))
    }
    (1 to 100) foreach { _ =>
      liveBus.post(Test1KyuubiEvent("test1"))
    }
  }

  test("register event handler for default bus") {
    EventBus.register[Test0KyuubiEvent] { e =>
      assert(e.content == "test0")
    }
    EventBus.register[Test1KyuubiEvent] { e =>
      assert(e.content == "test1")
    }

    class Test0Handler extends EventHandler[Test0KyuubiEvent] {
      override def apply(e: Test0KyuubiEvent): Unit = {
        assert(e.content == "test0")
      }
    }

    EventBus.register[Test0KyuubiEvent](new Test0Handler)

    EventBus.post(Test0KyuubiEvent("test0"))
    EventBus.post(Test1KyuubiEvent("test1"))
  }

  test("combine with logging service") {

    class Test1EventLogger extends EventLogger[Test1KyuubiEvent] {
      override def logEvent(event: Test1KyuubiEvent): Unit = {
        assert(event.content == "test1 logger service")
      }
    }

    class Test1LogService extends AbstractEventLoggingService[Test1KyuubiEvent]
      with EventHandler[Test1KyuubiEvent] {

      override def apply(e: Test1KyuubiEvent): Unit = {
        onEvent(e)
      }

      override def initialize(conf: KyuubiConf): Unit = {
        val testEventLogger = new Test1EventLogger
        addEventLogger(testEventLogger)
        super.initialize(conf)
      }
    }

    val testLogService = new Test1LogService()
    testLogService.initialize(new KyuubiConf())

    EventBus.register[Test1KyuubiEvent](testLogService)
    EventBus.post(Test1KyuubiEvent("test1 logger service"))
  }
}
