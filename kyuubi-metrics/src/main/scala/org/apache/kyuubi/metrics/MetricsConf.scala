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

import java.time.Duration

import org.apache.kyuubi.config.{ConfigBuilder, ConfigEntry, KyuubiConf}
import org.apache.kyuubi.metrics.ReporterType._

object MetricsConf {

  private def buildConf(key: String): ConfigBuilder = KyuubiConf.buildConf(key)

  val METRICS_ENABLED: ConfigEntry[Boolean] =
    buildConf("metrics.enabled")
      .doc("Set to true to enable kyuubi metrics system")
      .version("1.1.0")
      .booleanConf
      .createWithDefault(true)

  val METRICS_REPORTERS: ConfigEntry[Seq[String]] = buildConf("metrics.reporters")
    .doc("A comma separated list for all metrics reporters<ul>" +
      " <li>JSON - default reporter which outputs measurements to json file periodically</li>" +
      " <li>CONSOLE - ConsoleReporter which outputs measurements to CONSOLE.</li>" +
      " <li>SLF4J - Slf4jReporter which outputs measurements to system log.</li>" +
      " <li>JMX - JmxReporter which listens for new metrics and exposes them as namespaced" +
      " MBeans.</li> </ul>")
    .version("1.1.0")
    .stringConf
    .transform(_.toUpperCase())
    .toSequence
    .createWithDefault(Seq(JSON.toString))

  val METRICS_REPORT_LOCATION: ConfigEntry[String] = buildConf("metrics.json.report.location")
    .doc("Where the json metrics file located")
    .version("1.1.0")
    .stringConf
    .createWithDefault("metrics")

  val METRICS_REPORT_INTERVAL: ConfigEntry[Long] = buildConf("metrics.report.interval")
    .doc("How often should report metrics to json/console. no effect on JMX")
    .version("1.1.0")
    .timeConf
    .createWithDefault(Duration.ofSeconds(5).toMillis)
}
