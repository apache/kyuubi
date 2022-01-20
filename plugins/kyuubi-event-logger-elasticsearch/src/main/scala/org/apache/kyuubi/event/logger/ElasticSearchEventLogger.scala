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

package org.apache.kyuubi.event.logger

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.event.logger.ElasticSearchEventLoggerConf._
import org.apache.kyuubi.event.logger.elasticsearch.ElasticSearchEventIndex
import org.apache.kyuubi.events.{EventLogger, KyuubiEvent}
import org.apache.kyuubi.service.AbstractService

class ElasticSearchEventLogger[T <: KyuubiEvent]
  extends AbstractService("ElasticSearchEventLogger")
    with EventLogger[T] with Logging {

  private var eventIndex: ElasticSearchEventIndex = _

  override def logEvent(kyuubiEvent: T): Unit = {
    try {
      this.eventIndex.index(kyuubiEvent)
    } catch {
      case e: Throwable => warn("Elasticsearch log event failed.", e)
    }
  }

  /**
   * Initialize the service.
   *
   * The transition must be from [[LATENT]]to [[INITIALIZED]] unless the
   * operation failed and an exception was raised.
   *
   * @param conf the configuration of the service
   */
  override def initialize(conf: KyuubiConf): Unit = synchronized {
    this.eventIndex = ElasticSearchEventIndex(
      conf.get(ELASTICSEARCH_EVENT_LOGGER_HOSTS),
      conf.get(ELASTICSEARCH_EVENT_LOGGER_USERNAME),
      conf.get(ELASTICSEARCH_EVENT_LOGGER_PASSWORD),
      conf.get(ELASTICSEARCH_EVENT_LOGGER_INDICES))

    this.eventIndex.createIndices()
    super.initialize(conf)
  }

  /**
   * Stop the service.
   *
   * This operation must be designed to complete regardless of the initial state
   * of the service, including the state of all its internal fields.
   */
  override def stop(): Unit = synchronized {
    this.eventIndex.close()
    super.stop()
  }

}
