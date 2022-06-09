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

package org.apache.kyuubi.server.statestore

import java.util.concurrent.{ConcurrentHashMap, ThreadPoolExecutor, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.util.ThreadUtils

class StateStoreRequestRetryManager private (stateStore: SessionStateStore, name: String)
  extends AbstractService(name) with Logging {

  def this(stateStore: SessionStateStore) =
    this(stateStore, classOf[StateStoreRequestRetryManager].getSimpleName)

  private val identifierRequestsRetryRefMap =
    new ConcurrentHashMap[String, StateStoreRequestsRetryRef]()

  private val retryTrigger =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("state-store-requests-retry-trigger")

  private val retryExecutor: ThreadPoolExecutor =
    ThreadUtils.newDaemonFixedThreadPool(100, "state-store-requests-retry-executor")

  override def initialize(conf: KyuubiConf): Unit = {
    super.initialize(conf)
  }

  override def start(): Unit = {
    startStateStoreRequestsRetryTrigger()
    super.start()
  }

  override def stop(): Unit = {
    super.stop()
    ThreadUtils.shutdown(retryTrigger)
    ThreadUtils.shutdown(retryExecutor)
  }

  def getOrCreateStateStoreRequestsRetryRef(identifier: String): StateStoreRequestsRetryRef = {
    identifierRequestsRetryRefMap.computeIfAbsent(
      identifier,
      identifier => {
        val ref = new StateStoreRequestsRetryRef(identifier)
        debug(s"Created StateStoreRequestsRetryRef for session $identifier.")
        ref
      })
  }

  def removeStateStoreRequestsRetryRef(identifier: String): Unit = {
    identifierRequestsRetryRefMap.remove(identifier)
  }

  private def startStateStoreRequestsRetryTrigger(): Unit = {
    val interval = conf.get(KyuubiConf.SERVER_STATE_STORE_REQUESTS_RETRY_INTERVAL)
    val triggerTask = new Runnable {
      override def run(): Unit = {
        identifierRequestsRetryRefMap.values().asScala.foreach { ref =>
          if (ref.hasRemainingRequests() && ref.retryingTaskCount.get() == 0) {
            val retryTask = new Runnable {
              override def run(): Unit = {
                try {
                  info(s"Retrying state store requests for" +
                    s" ${ref.identifier}/${ref.retryCount.incrementAndGet()}")
                  var stateEvent = ref.stateStoreRequestQueue.peek()
                  while (stateEvent != null) {
                    stateEvent match {
                      case insert: RetryingInsertSessionMetadata =>
                        stateStore.insertMetadata(insert.metadata)

                      case update: RetryingUpdateSessionMetadata =>
                        stateStore.updateMetadata(update.metadata)

                      case _ =>
                    }
                    ref.stateStoreRequestQueue.remove(stateEvent)
                    stateEvent = ref.stateStoreRequestQueue.peek()
                  }
                } catch {
                  case e: Throwable =>
                    error(
                      s"Error retrying state store requests for" +
                        s" ${ref.identifier}/${ref.retryCount.get()}",
                      e)
                } finally {
                  ref.retryingTaskCount.decrementAndGet()
                }
              }
            }

            try {
              ref.retryingTaskCount.incrementAndGet()
              retryExecutor.submit(retryTask)
            } catch {
              case e: Throwable =>
                error(s"Error submitting retrying state store requests for ${ref.identifier}", e)
                ref.retryingTaskCount.decrementAndGet()
            }
          }
        }
      }
    }
    retryTrigger.scheduleWithFixedDelay(triggerTask, interval, interval, TimeUnit.MILLISECONDS)
  }
}
