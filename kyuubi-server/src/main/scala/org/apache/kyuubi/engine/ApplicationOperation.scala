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

package org.apache.kyuubi.engine

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.ApplicationState.ApplicationState

trait ApplicationOperation {

  /**
   * Step for initializing the instance.
   */
  def initialize(conf: KyuubiConf): Unit

  /**
   * Step to clean up the instance
   */
  def stop(): Unit

  /**
   * Called before other method to do a quick skip
   *
   * @param clusterManager the underlying cluster manager or just local instance
   */
  def isSupported(clusterManager: Option[String]): Boolean

  /**
   * Kill the app/engine by the unique application tag
   *
   * @param tag the unique application tag for engine instance.
   *            For example,
   *            if the Hadoop Yarn is used, for spark applications,
   *            the tag will be preset via spark.yarn.tags
   * @return a message contains response describing how the kill process.
   *
   * @note For implementations, please suppress exceptions and always return KillResponse
   */
  def killApplicationByTag(tag: String): KillResponse

  /**
   * Get the engine/application status by the unique application tag
   *
   * @param tag the unique application tag for engine instance.
   * @return [[ApplicationInfo]]
   */
  def getApplicationInfoByTag(tag: String): ApplicationInfo
}

object ApplicationState extends Enumeration {
  type ApplicationState = Value
  val PENDING, RUNNING, FINISHED, KILLED, FAILED, ZOMBIE, NOT_FOUND = Value
}

case class ApplicationInfo(
    id: String,
    name: String,
    state: ApplicationState,
    url: Option[String] = None,
    error: Option[String] = None) {

  def toKeyValueList: Seq[Seq[String]] = {
    val values = new Iterator[String] {
      private val original = productIterator
      override def hasNext: Boolean = {
        original.hasNext
      }

      override def next(): String = {
        original.next() match {
          case null => null
          case None => null
          case Some(v) => v.toString
          case e: Enumeration => e.toString()
          case other => other.toString
        }
      }
    }
    Seq(Seq("id", "name", "state", "url", "error"), values.toSeq)
  }
}

object ApplicationOperation {
  val NOT_FOUND = "APPLICATION_NOT_FOUND"
}
