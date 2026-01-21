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

import org.apache.kyuubi.config.ConfigEntry
import org.apache.kyuubi.config.KyuubiConf.buildConf
import org.apache.kyuubi.metrics.ReporterType._

object MetricsConf {

  val METRICS_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.metrics.enabled")
      .serverOnly
      .doc("Set to true to enable kyuubi metrics system")
      .version("1.2.0")
      .booleanConf
      .createWithDefault(true)

  val METRICS_REPORTERS: ConfigEntry[Set[String]] = buildConf("kyuubi.metrics.reporters")
    .serverOnly
    .doc("A comma-separated list for all metrics reporters" +
      "<ul>" +
      " <li>CONSOLE - ConsoleReporter which outputs measurements to CONSOLE periodically.</li>" +
      " <li>JMX - JmxReporter which listens for new metrics and exposes them as MBeans.</li> " +
      " <li>JSON - JsonReporter which outputs measurements to json file periodically.</li>" +
      " <li>PROMETHEUS - PrometheusReporter which exposes metrics in Prometheus format.</li>" +
      " <li>SLF4J - Slf4jReporter which outputs measurements to system log periodically.</li>" +
      "</ul>")
    .version("1.2.0")
    .stringConf
    .transformToUpperCase
    .toSet()
    .checkValues(ReporterType)
    .createWithDefault(Set(PROMETHEUS.toString))

  val METRICS_CONSOLE_INTERVAL: ConfigEntry[Long] = buildConf("kyuubi.metrics.console.interval")
    .serverOnly
    .doc("How often should report metrics to console")
    .version("1.2.0")
    .timeConf
    .createWithDefault(Duration.ofSeconds(5).toMillis)

  val METRICS_JSON_LOCATION: ConfigEntry[String] = buildConf("kyuubi.metrics.json.location")
    .serverOnly
    .doc("Where the JSON metrics file located. " +
      "Note: this value support the variables substitution: `{{KYUUBI_HOME}}`, " +
      "`{{KYUUBI_WORK_DIR_ROOT}}`.")
    .version("1.2.0")
    .stringConf
    .createWithDefault("{{KYUUBI_HOME}}/metrics")

  val METRICS_JSON_INTERVAL: ConfigEntry[Long] = buildConf("kyuubi.metrics.json.interval")
    .serverOnly
    .doc("How often should report metrics to JSON file")
    .version("1.2.0")
    .timeConf
    .createWithDefault(Duration.ofSeconds(5).toMillis)

  val METRICS_PROMETHEUS_PORT: ConfigEntry[Int] = buildConf("kyuubi.metrics.prometheus.port")
    .serverOnly
    .doc("Prometheus metrics HTTP server port")
    .version("1.2.0")
    .intConf
    .checkValue(p => p == 0 || (p > 1024 && p < 65535), "Invalid Port number")
    .createWithDefault(10019)

  val METRICS_PROMETHEUS_PATH: ConfigEntry[String] = buildConf("kyuubi.metrics.prometheus.path")
    .serverOnly
    .doc("URI context path of prometheus metrics HTTP server")
    .version("1.2.0")
    .stringConf
    .checkValue(path => path.startsWith("/"), "Context path must start with '/'")
    .createWithDefault("/metrics")

  val METRICS_PROMETHEUS_LABELS_INSTANCE_ENABLED: ConfigEntry[Boolean] =
    buildConf("kyuubi.metrics.prometheus.labels.instance.enabled")
      .serverOnly
      .doc("Whether to add instance label to prometheus metrics")
      .version("1.10.2")
      .booleanConf
      .createWithDefault(false)

  val METRICS_SLF4J_INTERVAL: ConfigEntry[Long] = buildConf("kyuubi.metrics.slf4j.interval")
    .serverOnly
    .doc("How often should report metrics to SLF4J logger")
    .version("1.2.0")
    .timeConf
    .createWithDefault(Duration.ofSeconds(5).toMillis)
}
