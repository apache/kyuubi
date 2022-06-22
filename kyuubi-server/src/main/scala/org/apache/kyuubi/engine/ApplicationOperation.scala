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

trait ApplicationOperation {

  /**
   * Step for initializing the instance.
   */
  def initialize(conf: KyuubiConf, kyuubiApplicationManager: KyuubiApplicationManager): Unit

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
   * @return a map contains the application status
   */
  def getApplicationInfoByTag(tag: String): Map[String, String]
}

object ApplicationOperation {

  /**
   * identifier determined by cluster manager for the engine
   */
  val APP_ID_KEY = "id"
  val APP_NAME_KEY = "name"
  val APP_STATE_KEY = "state"
  val APP_URL_KEY = "url"
  val APP_ERROR_KEY = "error"

  val NOT_FOUND = "APPLICATION_NOT_FOUND"
}
