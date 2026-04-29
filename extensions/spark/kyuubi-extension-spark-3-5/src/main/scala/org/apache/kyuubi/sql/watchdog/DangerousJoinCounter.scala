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

package org.apache.kyuubi.sql.watchdog

import scala.collection.mutable.ArrayBuffer

object DangerousJoinCounter {
  case class Entry(
      sqlText: String,
      joinType: String,
      reason: String,
      leftSize: BigInt,
      rightSize: BigInt,
      broadcastThreshold: Long,
      broadcastRatio: Double) {
    def toJson: String = {
      val pairs = Seq(
        "sql" -> escape(sqlText),
        "joinType" -> escape(joinType),
        "reason" -> escape(reason),
        "leftSize" -> leftSize.toString,
        "rightSize" -> rightSize.toString,
        "broadcastThreshold" -> broadcastThreshold.toString,
        "broadcastRatio" -> broadcastRatio.toString)
      pairs.map { case (k, v) =>
        if (k == "leftSize" || k == "rightSize" || k == "broadcastThreshold" || k == "broadcastRatio") {
          s""""$k":$v"""
        } else {
          s""""$k":"$v""""
        }
      }.mkString("{", ",", "}")
    }
  }

  private val entries = ArrayBuffer.empty[Entry]

  def add(entry: Entry): Unit = synchronized {
    entries += entry
  }

  def count: Int = synchronized {
    entries.size
  }

  def latest: Option[Entry] = synchronized {
    entries.lastOption
  }

  def snapshot: Seq[Entry] = synchronized {
    entries.toSeq
  }

  def reset(): Unit = synchronized {
    entries.clear()
  }

  private def escape(raw: String): String = {
    raw
      .replace("\\", "\\\\")
      .replace("\"", "\\\"")
      .replace("\n", "\\n")
      .replace("\r", "\\r")
      .replace("\t", "\\t")
  }
}
