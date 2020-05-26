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

package org.apache.kyuubi.service

import org.apache.kyuubi.config.KyuubiConf

trait Service {
  import ServiceState._
  /**
   * Initialize the service.
   *
   * The transition must be from [[NEW]]to [[INITIALIZED]] unless the
   * operation failed and an exception was raised.
   *
   * @param conf the configuration of the service
   */
  def initialize(conf: KyuubiConf): Unit

  /**
   * Start the service.
   *
   * The transition should be from [[INITIALIZED]] to [[STARTED]] unless the
   * operation failed and an exception was raised.
   */
  def start(): Unit

  /**
   * Stop the service.
   *
   * This operation must be designed to complete regardless of the initial state
   * of the service, including the state of all its internal fields.
   */
  def stop(): Unit

  /**
   * Get the name of this service.
   *
   * @return the service name
   */
  def getName: String

  /**
   * Get the configuration of this service.
   * This is normally not a clone and may be manipulated, though there are no
   * guarantees as to what the consequences of such actions may be
   *
   * @return the current configuration, unless a specific implementation chooses
   *         otherwise.
   */
  def getConf: KyuubiConf

  /**
   * Get the current service state
   *
   * @return the state of the service
   */
  def getServiceState: ServiceState

  /**
   * Get the service start time
   *
   * @return the start time of the service. This will be zero if the service
   *         has not yet been started.
   */
  def getStartTime: Long
}
