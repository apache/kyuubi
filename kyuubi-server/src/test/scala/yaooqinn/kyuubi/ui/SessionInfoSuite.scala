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

package yaooqinn.kyuubi.ui

import java.util.UUID

import org.apache.spark.{KyuubiSparkUtil, SparkFunSuite}

class SessionInfoSuite extends SparkFunSuite {

  test("session info") {
    val sessionId = UUID.randomUUID().toString
    val ip = KyuubiSparkUtil.localHostName()
    val startTimeStamp = System.currentTimeMillis() - 1000L
    val user = KyuubiSparkUtil.getCurrentUserName
    val info = new SessionInfo(sessionId, startTimeStamp, ip, user)
    assert(info.finishTimestamp === 0L)
    assert(info.totalExecution === 0)
    assert(info.totalTime !== info.finishTimestamp - startTimeStamp)
    info.finishTimestamp = System.currentTimeMillis()
    assert(info.totalTime === info.finishTimestamp - startTimeStamp)
  }
}
