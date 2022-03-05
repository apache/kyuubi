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

package org.apache.kyuubi.server

import java.net.InetAddress

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{SERVER_EVENT_JSON_LOG_PATH, SERVER_EVENT_LOGGERS}
import org.apache.kyuubi.events.{AbstractEventLoggingService, EventLoggerType, JsonEventLogger}
import org.apache.kyuubi.util.KyuubiHadoopUtils

class KyuubiEventLoggingService extends AbstractEventLoggingService {

  override def initialize(conf: KyuubiConf): Unit = {
    val hadoopConf = KyuubiHadoopUtils.newHadoopConf(conf)
    conf.get(SERVER_EVENT_LOGGERS)
      .map(EventLoggerType.withName)
      .foreach {
        case EventLoggerType.JSON =>
          val hostName = InetAddress.getLocalHost.getCanonicalHostName
          val jsonEventLogger = new JsonEventLogger(
            s"server-$hostName",
            SERVER_EVENT_JSON_LOG_PATH,
            hadoopConf)

          jsonEventLogger.createEventLogRootDir(conf, hadoopConf)
          addService(jsonEventLogger)
          addEventLogger(jsonEventLogger)
        case logger =>
          // TODO: Add more implementations
          throw new IllegalArgumentException(s"Unrecognized event logger: $logger")
      }
    super.initialize(conf)
  }
}
