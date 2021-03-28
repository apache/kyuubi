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

package org.apache.kyuubi.metrics.micrometer

import java.time.Duration
import java.util.Properties

import io.micrometer.core.instrument.composite.CompositeMeterRegistry
import io.micrometer.core.instrument.logging.LoggingMeterRegistry
import org.slf4j.LoggerFactory

import org.apache.kyuubi.metrics.MetricsConf._
import org.apache.kyuubi.service.AbstractService

class Slf4jMetricsExporterService(registry: CompositeMeterRegistry)
  extends AbstractService("Slf4jMetricsExporterService") {

  override def start(): Unit = {
    val metricsLogger = LoggerFactory.getLogger(conf.get(METRICS_SLF4J_LOGGER))

    val props = new Properties
    props.setProperty("logging.step", Duration.ofMillis(conf.get(METRICS_SLF4J_INTERVAL)).toString)
    props.setProperty("logging.logInactive", conf.get(METRICS_SLF4J_LOG_INACTIVE).toString)

    val loggingMeterRegistry = LoggingMeterRegistry.builder(key => props.getProperty(key))
      .loggingSink(metricsLogger.info(_))
      .build

    registry.add(loggingMeterRegistry)
    super.start()
  }
}
