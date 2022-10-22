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
package org.apache.kyuubi.engine.reblance

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.kyuubi.KyuubiFunSuite

class RebalancedEnginePolicyTest extends KyuubiFunSuite {

  test("no expand init") {
    val subdomainPrefix = "engine-pool"
    val enginExpandThreshold = 3
    val spaceSessions = mutable.Map[String, ListBuffer[String]]()
    val res = RebalancedEnginePolicy.chooseAdaptiveSpace(
      subdomainPrefix,
      enginExpandThreshold,
      spaceSessions.toMap)
    assert(res.spaceIndex == 1)
  }

  test("do not need expand engin space") {
    val subdomainPrefix = "engine-pool"
    val enginExpandThreshold = 3
    val spaceSessions = mutable.Map[String, ListBuffer[String]]()
    val space1Handles = ListBuffer[String]()
    val allSpaces = ListBuffer[String]()

    allSpaces += "engine-pool-1"
    space1Handles += "handle0"
    space1Handles += "handle1"
    spaceSessions += ("engine-pool-1" -> space1Handles)

    val res = RebalancedEnginePolicy.chooseAdaptiveSpace(
      subdomainPrefix,
      enginExpandThreshold,
      spaceSessions.toMap)
    assert(res.adaptiveSpaceStr.equals("engine-pool-1"))
  }

  test("balance to max load space") {
    val subdomainPrefix = "engine-pool"
    val enginExpandThreshold = 3
    val spaceSessions = mutable.Map[String, ListBuffer[String]]()
    val space1Handles = ListBuffer[String]()
    val space2Handles = ListBuffer[String]()
    val allSpaces = ListBuffer[String]()

    allSpaces += "engine-pool-1"
    space1Handles += "handle0"
    space1Handles += "handle1"
    space1Handles += "handle2"
    spaceSessions += ("engine-pool-1" -> space1Handles)

    allSpaces += "engine-pool-2"
    space2Handles += "handle0"
    space2Handles += "handle1"
    spaceSessions += ("engine-pool-2" -> space2Handles)

    val res = RebalancedEnginePolicy.chooseAdaptiveSpace(
      subdomainPrefix,
      enginExpandThreshold,
      spaceSessions.toMap)
    assert(res.adaptiveSpaceStr.equals("engine-pool-2"))
  }

  test("expand new engin space suitcase 1") {
    val subdomainPrefix = "engine-pool"
    val enginExpandThreshold = 3
    val spaceSessions = mutable.Map[String, ListBuffer[String]]()
    val space1Handles = ListBuffer[String]()
    val allSpaces = ListBuffer[String]()

    allSpaces += "engine-pool-1"
    space1Handles += "handle0"
    space1Handles += "handle1"
    space1Handles += "handle2"
    spaceSessions += ("engine-pool-1" -> space1Handles)

    val res = RebalancedEnginePolicy.chooseAdaptiveSpace(
      subdomainPrefix,
      enginExpandThreshold,
      spaceSessions.toMap)
    assert(res.adaptiveSpaceStr.equals("engine-pool-2"))
  }

  test("expand engin space suitcase 2") {
    val subdomainPrefix = "engine-pool"
    val enginExpandThreshold = 3
    val spaceSessions = mutable.Map[String, ListBuffer[String]]()
    val space1Handles = ListBuffer[String]()
    val space2Handles = ListBuffer[String]()
    val allSpaces = ListBuffer[String]()

    allSpaces += "engine-pool-1"
    space1Handles += "handle0"
    space1Handles += "handle1"
    space1Handles += "handle2"
    spaceSessions += ("engine-pool-1" -> space1Handles)

    allSpaces += "engine-pool-2"
    space2Handles += "handle0"
    space2Handles += "handle1"
    space2Handles += "handle2"
    spaceSessions += ("engine-pool-2" -> space2Handles)

    val res = RebalancedEnginePolicy.chooseAdaptiveSpace(
      subdomainPrefix,
      enginExpandThreshold,
      spaceSessions.toMap)
    assert(res.adaptiveSpaceStr.equals("engine-pool-3"))
  }

}
