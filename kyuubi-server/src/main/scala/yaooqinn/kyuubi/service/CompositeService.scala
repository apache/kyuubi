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

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkConf

import yaooqinn.kyuubi.Logging

/**
 * CompositeService.
 *
 */
class CompositeService(name: String) extends AbstractService(name) with Logging {

  private[this] final val serviceList = new ListBuffer[Service]

  def getServices: List[Service] = serviceList.toList

  protected def addService(service: Service): Unit = {
    serviceList.+=(service)
    info(s"Service: ${service.getName} is added.")
  }

  override def init(conf: SparkConf): Unit = {
    for (service <- serviceList) {
      service.init(conf)
    }
    super.init(conf)
  }

  override def start(): Unit = {
    serviceList.zipWithIndex.foreach { case (service, i) =>
      try {
        service.start()
      } catch {
        case e: Throwable =>
          error("Error starting services " + getName, e)
          stop(i)
          throw new ServiceException("Failed to Start " + getName, e)
      }
    }
    super.start()
  }

  override def stop(): Unit = {
    this.getServiceState match {
      case State.STOPPED =>
      case _ =>
        if (serviceList.nonEmpty) {
          stop(serviceList.size)
        }
        super.stop()
    }
  }

  /**
   * stop in reserve order of start
   * @param numOfServicesStarted num of services which at start state
   */
  private def stop(numOfServicesStarted: Int): Unit = {
    // stop in reserve order of start
    serviceList.toList.take(numOfServicesStarted).reverse.foreach { service =>
      try {
        info(s"Service: [${service.getName}] is stopping.")
        service.stop()
      } catch {
        case _: Throwable =>
          info("Error stopping " + service.getName)
      }
    }
  }
}
