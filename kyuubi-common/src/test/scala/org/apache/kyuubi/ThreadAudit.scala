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

package org.apache.kyuubi

import scala.collection.JavaConverters._

trait ThreadAudit extends Logging {
  private var threadNamesSnapshot: Set[String] = Set.empty

  protected def doThreadPreAudit(): Unit = {
    threadNamesSnapshot = runningThreadNames()
  }

  protected def doThreadPostAudit(): Unit = {
    val shortName = this.getClass.getName.replaceAll("org.apache.kyuubi", "o.a.k")
    if (threadNamesSnapshot.nonEmpty) {
      val remainingThreadNames = runningThreadNames().diff(threadNamesSnapshot)
        .filterNot { s => "process reaper" == s }
      if (remainingThreadNames.nonEmpty) {
        warn(s"\n\n===== POSSIBLE THREAD LEAK IN SUITE $shortName, " +
          s"thread names: ${remainingThreadNames.mkString(", ")} =====\n")
      }
    } else {
      warn(s"\n\n= THREAD AUDIT POST ACTION CALLED WITHOUT PRE ACTION IN SUITE $shortName ==\n")
    }
  }

  private def runningThreadNames(): Set[String] = {
    Thread.getAllStackTraces.keySet().asScala.map(_.getName).toSet
  }
}
