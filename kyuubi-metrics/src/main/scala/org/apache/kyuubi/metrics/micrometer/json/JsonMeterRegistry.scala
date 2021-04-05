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

package org.apache.kyuubi.metrics.micrometer.json

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.nio.file.attribute.PosixFilePermissions
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.ScalaObjectMapper
import io.micrometer.core.instrument._
import io.micrometer.core.instrument.config.NamingConvention
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig
import io.micrometer.core.instrument.distribution.pause.PauseDetector
import io.micrometer.core.instrument.step.{StepDistributionSummary, StepMeterRegistry, StepTimer}
import io.micrometer.core.instrument.util.NamedThreadFactory

class JsonMeterRegistry(config: JsonRegistryConfig = JsonRegistryConfig.DEFAULT,
                        clock: Clock = Clock.SYSTEM) extends StepMeterRegistry(config, clock) {

  private val reportDir = Paths.get(config.location).toAbsolutePath
  private val reportPath = Paths.get(config.location, "report.json").toAbsolutePath

  config().namingConvention(NamingConvention.dot)

  private[json] lazy val om: ObjectMapper with ScalaObjectMapper = {
    val _om = new ObjectMapper() with ScalaObjectMapper
    MicrometerSerializers.registerSerializers(_om, config.step, config().namingConvention)
    _om
  }

  start(new NamedThreadFactory("logging-metrics-publisher"))

  override protected def publish(): Unit = {
    if (!config.enabled) return

    val tmpPath = Files.createTempFile(reportDir, "report", ".json").toAbsolutePath

    val writer = Files.newBufferedWriter(tmpPath, StandardCharsets.UTF_8)
    getMeters.asScala
      .sorted(Ordering[(Meter.Type, String)].on((m: Meter) => (m.getId.getType, m.getId.getName)))
      .foreach { m: Meter =>
        writer.write(om.writeValueAsString(m))
        writer.newLine()
      }
    writer.close()

    Files.setPosixFilePermissions(tmpPath, PosixFilePermissions.fromString("rwxr--r--"))
    Files.move(tmpPath, reportPath, StandardCopyOption.REPLACE_EXISTING)
  }

  override protected def newTimer(id: Meter.Id,
                                  distributionStatisticConfig: DistributionStatisticConfig,
                                  pauseDetector: PauseDetector): Timer =
    new StepTimer(id, clock, distributionStatisticConfig, pauseDetector, getBaseTimeUnit,
      this.config.step.toMillis, true)

  override def newDistributionSummary(id: Meter.Id,
                                      distributionStatisticConfig: DistributionStatisticConfig,
                                      scale: Double): DistributionSummary =
    new StepDistributionSummary(id, clock, distributionStatisticConfig, scale,
      this.config.step.toMillis, true)

  override protected def getBaseTimeUnit = TimeUnit.MILLISECONDS
}
