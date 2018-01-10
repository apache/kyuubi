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

package yaooqinn.kyuubi.ui

import scala.collection.mutable.ArrayBuffer

class ExecutionInfo(
    val statement: String,
    val sessionId: String,
    val startTimestamp: Long,
    val userName: User) {
  var finishTimestamp: Long = 0L
  var executePlan: String = ""
  var detail: String = ""
  var state: ExecutionState.Value = ExecutionState.STARTED
  val jobId: ArrayBuffer[String] = ArrayBuffer[String]()
  var groupId: String = ""
  def totalTime: Long = {
    if (finishTimestamp == 0L) {
      System.currentTimeMillis - startTimestamp
    } else {
      finishTimestamp - startTimestamp
    }
  }
}
