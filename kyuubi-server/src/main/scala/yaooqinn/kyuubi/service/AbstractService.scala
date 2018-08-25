/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.service

import org.apache.spark.SparkConf

import yaooqinn.kyuubi.Logging

/**
 * AbstractService.
 *
 */
abstract class AbstractService(name: String) extends Service with Logging {

  /**
   * Service state: initially [[State.NOT_INITED]].
   */
  private var state = State.NOT_INITED

  /**
   * Service start time. Will be zero until the service is started.
   */
  private var startTime: Long = 0L

  /**
   * The configuration. Will be null until the service is initialized.
   */
  protected var conf: SparkConf = _

  /**
   * Get the current service state
   *
   * @return the state of the service
   */
  override def getServiceState: State.Value = state

  /**
   * Initialize the service.
   *
   * @throws IllegalStateException if the current service state does not permit this action
   */
  override def init(conf: SparkConf): Unit = {
    ensureCurrentState(State.NOT_INITED)
    this.conf = conf
    changeState(State.INITED)
    info("Service: [" + getName + "] is initialized.")
  }

  override def start(): Unit = {
    ensureCurrentState(State.INITED)
    startTime = System.currentTimeMillis
    changeState(State.STARTED)
    info("Service: [" + getName + "] is started.")
  }

  override def stop(): Unit = {
    state match {
      case State.STOPPED =>
      case State.NOT_INITED =>
      case State.INITED =>
      case _ =>
        ensureCurrentState(State.STARTED)
        changeState(State.STOPPED)
        info("Service: [" + getName + "] is stopped.")
    }
  }

  override def getName: String = name

  override def getConf: SparkConf = conf

  override def getStartTime: Long = startTime

  /**
   * Verify that that a service is in a given state.
   *
   * @param currentState the desired state
   *
   * @throws IllegalStateException if the service state is different from the desired state
   */
  private def ensureCurrentState(currentState: State.Value): Unit = {
    if (state ne currentState) {
      throw new IllegalStateException(
        s"""
           |For this operation, the current service state must be $state instead of $currentState
         """.stripMargin)
    }
  }

  /**
   * Change to a new state.
   *
   * @param newState new service state
   */
  private def changeState(newState: State.Value): Unit = {
    state = newState
  }
}
