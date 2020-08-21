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

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf

abstract class AbstractService(serviceName: String) extends Service with Logging {
  import ServiceState._
  protected var conf: KyuubiConf = _
  protected var state: ServiceState = LATENT
  protected var startTime: Long = _

  /**
   * Initialize the service.
   *
   * The transition must be from [[LATENT]]to [[INITIALIZED]] unless the
   * operation failed and an exception was raised.
   *
   * @param conf the configuration of the service
   */
  override def initialize(conf: KyuubiConf): Unit = {
    ensureCurrentState(LATENT)
    this.conf = conf
    changeState(INITIALIZED)
    info(s"Service[$serviceName] is initialized.")
  }

  /**
   * Start the service.
   *
   * The transition should be from [[INITIALIZED]] to [[STARTED]] unless the
   * operation failed and an exception was raised.
   */
  override def start(): Unit = {
    ensureCurrentState(INITIALIZED)
    this.startTime = System.currentTimeMillis()
    changeState(STARTED)
    info(s"Service[$serviceName] is started.")
  }

  /**
   * Stop the service.
   *
   * This operation must be designed to complete regardless of the initial state
   * of the service, including the state of all its internal fields.
   */
  override def stop(): Unit = {
    state match {
      case LATENT | INITIALIZED | STOPPED =>
      case _ =>
        ensureCurrentState(STARTED)
        changeState(STOPPED)
        info(s"Service[$serviceName] is stopped.")
    }
  }

  /**
   * Get the name of this service.
   *
   * @return the service name
   */
  override def getName: String = serviceName

  /**
   * Get the configuration of this service.
   * This is normally not a clone and may be manipulated, though there are no
   * guarantees as to what the consequences of such actions may be
   *
   * @return the current configuration, unless a specific implementation chooses
   *         otherwise.
   */
  override def getConf: KyuubiConf = conf

  /**
   * Get the current service state
   *
   * @return the state of the service
   */
  override def getServiceState: ServiceState = state

  /**
   * Get the service start time
   *
   * @return the start time of the service. This will be zero if the service
   *         has not yet been started.
   */
  override def getStartTime: Long = startTime

  /**
   * Verify that that a service is in a given state.
   *
   * @param currentState the desired state
   *
   * @throws IllegalStateException if the service state is different from the desired state
   */
  private def ensureCurrentState(currentState: ServiceState): Unit = {
    if (state ne currentState) {
      throw new IllegalStateException(
        s"""
           |For this operation, the current service state must be $currentState instead of $state
         """.stripMargin)
    }
  }

  /**
   * Change to a new state.
   *
   * @param newState new service state
   */
  private def changeState(newState: ServiceState): Unit = {
    state = newState
  }
}
