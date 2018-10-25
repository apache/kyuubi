/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.cli

import org.apache.hive.service.cli.thrift.THandleIdentifier
import org.apache.spark.SparkFunSuite

class HandleSuite extends SparkFunSuite {
  test("handle equality test") {
    val handle1 = TestHandle1()
    val handle2 = TestHandle2(new HandleIdentifier(handle1.handleId.toTHandleIdentifier))
    val handle3 = TestHandle3(handle2.handleId.toTHandleIdentifier)
    assert(handle1 === handle2)
    assert(handle1 === handle3)
    val handle4 = TestHandle2(null)
    assert(handle4.hashCode === 31)
    assert(!handle4.equals(null))
    assert(!handle4.equals(new Object))
    assert(!handle2.equals(handle4))
    assert(handle4.equals(handle4))
    assert(!handle4.equals(handle2))
    assert(handle2.equals(handle2))
  }

}

case class TestHandle1() extends Handle

case class TestHandle2(override val handleId: HandleIdentifier) extends Handle(handleId)

case class TestHandle3(tHandleIdentifier: THandleIdentifier) extends Handle(tHandleIdentifier)