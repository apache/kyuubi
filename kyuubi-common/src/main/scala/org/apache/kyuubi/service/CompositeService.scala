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

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.config.KyuubiConf

abstract class CompositeService(serviceName: String)
  extends AbstractService(serviceName) with Logging {

  import ServiceState.STOPPED

  private final val serviceList = new ArrayBuffer[Service]

  def getServices: Seq[Service] = serviceList.toList

  protected def addService(service: Service): Unit = {
    serviceList += service
  }

  override def initialize(conf: KyuubiConf): Unit = {
    serviceList.foreach(_.initialize(conf))
    super.initialize(conf)
  }

  override def start(): Unit = {
    serviceList.zipWithIndex.foreach { case (service, idx) =>
      try {
        service.start()
      } catch {
        case NonFatal(e) =>
          error(s"Error starting service ${service.getName}", e)
          stop(idx)
          throw new KyuubiException(s"Failed to Start $getName", e)
      }
    }
  }

  override def stop(): Unit = {
    getServiceState match {
      case STOPPED => warn(s"Service[$getName] is stopped already")
      case _ => stop(getServices.size)
    }
    super.stop()
  }

  /**
   * stop in reserve order of start
   * @param numOfServicesStarted num of services which at start state
   */
  private def stop(numOfServicesStarted: Int): Unit = {
    // stop in reserve order of start
    getServices.take(numOfServicesStarted).reverse.foreach { service =>
      try {
        info(s"Service: [${service.getName}] is stopping.")
        service.stop()
      } catch {
        case _: Throwable =>
          warn("Error stopping " + service.getName)
      }
    }
  }

}
