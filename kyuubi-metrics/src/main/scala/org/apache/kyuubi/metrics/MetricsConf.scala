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

import org.apache.kyuubi.config.{ConfigBuilder, ConfigEntry, KyuubiConf, OptionalConfigEntry}
import org.apache.kyuubi.metrics.ExporterType._

object MetricsConf {

  private def buildConf(key: String): ConfigBuilder = KyuubiConf.buildConf(key)

  val METRICS_ENABLED: ConfigEntry[Boolean] =
    buildConf("metrics.enabled")
      .doc("Set to true to enable kyuubi metrics system")
      .version("1.2.0")
      .booleanConf
      .createWithDefault(true)

  val METRICS_HISTOGRAM: ConfigEntry[Boolean] =
    buildConf("metrics.histogram")
      .doc("Set to true to enable publish percentiles histogram in summary and timer metrics")
      .version("1.2.0")
      .booleanConf
      .createWithDefault(true)

  val METRICS_EXPORTERS: ConfigEntry[Seq[String]] = buildConf("metrics.exporters")
    .doc("A comma separated list for all metrics exporters" +
      "<ul>" +
      " <li>JSON - Output measurements to file formatted by JSON each line.</li>" +
      " <li>SLF4J - Output measurements to system log.</li>" +
      " <li>PROMETHEUS - Expose measurements to Prometheus over HTTP server.</li>" +
      "</ul>")
    .version("1.2.0")
    .stringConf
    .transform(_.toUpperCase())
    .toSequence
    .createWithDefault(Seq(PROMETHEUS.toString))

  val METRICS_JSON_LOCATION: ConfigEntry[String] = buildConf("metrics.json.location")
    .doc("Where the json metrics file located")
    .version("1.2.0")
    .stringConf
    .createWithDefault("metrics")

  val METRICS_JSON_INTERVAL: ConfigEntry[Long] = buildConf("metrics.json.interval")
    .doc("How often should output measurements to json files. The default is 1 minute.")
    .version("1.2.0")
    .timeConf
    .createWithDefault(Duration.ofSeconds(60).toMillis)

  val METRICS_SLF4J_LOGGER: ConfigEntry[String] = buildConf("metrics.slf4j.logger")
    .doc("SLF4J metrics exporter logger name, default kyuubi.metrics")
    .version("1.2.0")
    .stringConf
    .createWithDefault("kyuubi.metrics")

  val METRICS_SLF4J_INTERVAL: ConfigEntry[Long] = buildConf("metrics.slf4j.interval")
    .doc("How often should output measurements to slf4j. The default is 1 minute.")
    .version("1.2.0")
    .timeConf
    .createWithDefault(Duration.ofSeconds(60).toMillis)

  val METRICS_SLF4J_LOG_INACTIVE: ConfigEntry[Boolean] =
    buildConf("metrics.slf4j.log.inactive")
      .doc("Whether counters and timers that have no activity in an interval are still logged.")
      .version("1.2.0")
      .booleanConf
      .createWithDefault(true)

  val METRICS_PROMETHEUS_HOST: OptionalConfigEntry[String] =
    buildConf("metrics.prometheus.host")
      .doc("HTTP Server hostname or IP of the machine on which to expose the prometheus metrics.")
      .version("1.2.0")
      .stringConf
      .createOptional

  val METRICS_PROMETHEUS_PORT: ConfigEntry[Int] = buildConf("metrics.prometheus.port")
    .doc("HTTP Server port of the machine on which to expose the prometheus metrics.")
    .version("1.2.0")
    .intConf
    .checkValue(p => p == 0 || (p > 1024 && p < 65535), "Invalid Port number")
    .createWithDefault(10090)

  val METRICS_PROMETHEUS_CONTEXT_PATH: ConfigEntry[String] =
    buildConf("metrics.prometheus.context.path")
      .doc("HTTP Server context path to expose the prometheus metrics.")
      .version("1.2.0")
      .stringConf
      .checkValue(
        path => path.startsWith("/"),
        "Context path must start with '/', e.g. '/metrics'")
      .createWithDefault("/metrics")

  val METRICS_PROMETHEUS_INTERVAL: ConfigEntry[Long] = buildConf("metrics.prometheus.interval")
    .doc("The step size to use in computing windowed statistics like max. To get the most " +
      "out of these statistics, align the step interval to be close to your scrape interval")
    .version("1.2.0")
    .timeConf
    .createWithDefault(Duration.ofSeconds(60).toMillis)

  val METRICS_PROMETHEUS_DESCRIPTION: ConfigEntry[Boolean] =
    buildConf("metrics.prometheus.description")
      .doc("Whether meter descriptions should be sent to Prometheus. " +
        "Turn this off to minimize the amount of data sent on each scrape")
      .version("1.2.0")
      .booleanConf
      .createWithDefault(true)
}
