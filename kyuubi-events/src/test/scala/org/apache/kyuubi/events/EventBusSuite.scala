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

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.events.handler.EventHandler

class EventBusSuite extends KyuubiFunSuite {

  case class Test0KyuubiEvent(content: String) extends KyuubiEvent {
    override def partitions: Seq[(String, String)] = Seq[(String, String)]()
  }

  case class Test1KyuubiEvent(content: String) extends KyuubiEvent {
    override def partitions: Seq[(String, String)] = Seq[(String, String)]()
  }

  trait TestKyuubiEvent extends KyuubiEvent {}

  case class Test2KyuubiEvent(name: String, content: String) extends TestKyuubiEvent {
    override def partitions: Seq[(String, String)] = Seq[(String, String)]()
  }

  case class Test3KyuubiEvent(content: String) extends KyuubiEvent {
    override def partitions: Seq[(String, String)] = Seq[(String, String)]()
  }

  test("register event handler") {
    var test0EventReceivedCount = 0
    var test1EventReceivedCount = 0
    var test2EventReceivedCount = 0
    var testEventReceivedCount = 0
    val liveBus = EventBus()

    liveBus.register[Test0KyuubiEvent] { e =>
      assert(e.content == "test0")
      assert(e.eventType == "test0_kyuubi")
      test0EventReceivedCount += 1
    }
    liveBus.register[Test1KyuubiEvent] { e =>
      assert(e.content == "test1")
      assert(e.eventType == "test1_kyuubi")
      test1EventReceivedCount += 1
    }
    // scribe subclass event
    liveBus.register[TestKyuubiEvent] { e =>
      assert(e.eventType == "test2_kyuubi")
      test2EventReceivedCount += 1
    }
    liveBus.register[KyuubiEvent] { _ =>
      testEventReceivedCount += 1
    }

    class Test0Handler extends EventHandler[Test0KyuubiEvent] {
      override def apply(e: Test0KyuubiEvent): Unit = {
        assert(e.content == "test0")
      }
    }

    liveBus.register[Test0KyuubiEvent](new Test0Handler)

    liveBus.register[Test1KyuubiEvent] { e =>
      assert(e.content == "test1")
    }

    (1 to 10) foreach { _ =>
      liveBus.post(Test0KyuubiEvent("test0"))
    }
    (1 to 20) foreach { _ =>
      liveBus.post(Test1KyuubiEvent("test1"))
    }
    (1 to 30) foreach { _ =>
      liveBus.post(Test2KyuubiEvent("name2", "test2"))
    }
    assert(test0EventReceivedCount == 10)
    assert(test1EventReceivedCount == 20)
    assert(test2EventReceivedCount == 30)
    assert(testEventReceivedCount == 60)
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

  test("async event handler") {
    val countDownLatch = new CountDownLatch(4)
    val count = new AtomicInteger(0)
    class Test0Handler extends EventHandler[Test0KyuubiEvent] {
      override def apply(e: Test0KyuubiEvent): Unit = {
        Thread.sleep(10)
        count.getAndIncrement()
        countDownLatch.countDown()
      }
    }
    class Test1Handler extends EventHandler[Test0KyuubiEvent] {
      override def apply(e: Test0KyuubiEvent): Unit = {
        count.getAndIncrement()
        countDownLatch.countDown()
      }
    }
    EventBus.registerAsync[Test0KyuubiEvent](new Test0Handler)
    EventBus.registerAsync[Test0KyuubiEvent](new Test1Handler)
    EventBus.post(Test0KyuubiEvent("test0"))
    EventBus.post(Test0KyuubiEvent("test1"))
    countDownLatch.await()
    assert(count.get() == 4)
  }
}
