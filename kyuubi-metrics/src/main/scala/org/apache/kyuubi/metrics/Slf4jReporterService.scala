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

package org.apache.kyuubi.metrics

import java.util.concurrent.TimeUnit

import com.codahale.metrics.{MetricRegistry, Slf4jReporter}

import org.apache.kyuubi.metrics.MetricsConf.METRICS_SLF4J_INTERVAL
import org.apache.kyuubi.service.AbstractService

class Slf4jReporterService(registry: MetricRegistry)
  extends AbstractService("Slf4jReporterService") {
  private var reporter: Slf4jReporter = _

  override def start(): Unit = synchronized {
    reporter = Slf4jReporter
      .forRegistry(registry)
      .outputTo(this.logger)
      .build()
    val interval = conf.get(METRICS_SLF4J_INTERVAL)
    reporter.start(interval, TimeUnit.MILLISECONDS)
    super.start()
  }

  override def stop(): Unit = synchronized {
    if (reporter != null) reporter.close()
    super.stop()
  }
}
