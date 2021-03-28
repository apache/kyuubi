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

import java.nio.file.{Files, Paths}
import java.time.Duration
import java.util.Properties

import io.micrometer.core.instrument.composite.CompositeMeterRegistry

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.metrics.MetricsConf._
import org.apache.kyuubi.metrics.micrometer.json.JsonMeterRegistry
import org.apache.kyuubi.service.AbstractService

class JsonMetricsExporterService(registry: CompositeMeterRegistry)
  extends AbstractService("JsonMetricsExporterService") {

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    val reportDir = Paths.get(conf.get(METRICS_JSON_LOCATION)).toAbsolutePath
    Files.createDirectories(reportDir)
    super.initialize(conf)
  }

  override def start(): Unit = {
    val props = new Properties
    props.setProperty("json.step", Duration.ofMillis(conf.get(METRICS_JSON_INTERVAL)).toString)
    props.setProperty("json.location", conf.get(METRICS_JSON_LOCATION))

    registry.add(new JsonMeterRegistry(key => props.getProperty(key)))

    super.start()
  }

  override def stop(): Unit = super.stop()
}
