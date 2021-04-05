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

import java.lang.{Double => BoxedDouble}
import java.text.DecimalFormat
import java.time.Duration
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.{ObjectMapper, SerializerProvider}
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import io.micrometer.core.instrument._
import io.micrometer.core.instrument.binder.BaseUnits
import io.micrometer.core.instrument.config.NamingConvention
import io.micrometer.core.instrument.distribution.{HistogramSnapshot, ValueAtPercentile}
import io.micrometer.core.instrument.step._

trait Helper {

  def writeId(gen: JsonGenerator, nc: NamingConvention, id: Meter.Id): Unit = {
    gen.writeStringField("name", id.getConventionName(nc))
    gen.writeObjectFieldStart("tags")
    id.getConventionTags(nc).forEach { t: Tag => gen.writeStringField(t.getKey, t.getValue) }
    gen.writeEndObject()
    gen.writeStringField("base_unit", id.getBaseUnit)
    gen.writeStringField("type", id.getType.name().toLowerCase)
  }

  def writeHistogramSnapshot(gen: JsonGenerator, snapshot: HistogramSnapshot,
                             step: Duration, timeUnit: Option[TimeUnit]): Unit = {
    gen.writeObjectField("count", snapshot.count)
    gen.writeObjectField("rate", snapshot.count / step.getSeconds)
    gen.writeStringField("rate_unit", "per_second")

    gen.writeObjectField("total", timeUnit.map(u => snapshot.total(u)).getOrElse(snapshot.total))
    gen.writeObjectField("mean", timeUnit.map(u => snapshot.mean(u)).getOrElse(snapshot.mean))
    gen.writeObjectField("max", timeUnit.map(u => snapshot.max(u)).getOrElse(snapshot.max))

    // write percentile, i.e. p50, p75, p95, p99, p999
    snapshot.percentileValues().foreach { p: ValueAtPercentile =>
      gen.writeObjectField(percentileKey(p.percentile),
        timeUnit.map(u => p.value(u)).getOrElse(p.value))
    }
    timeUnit.foreach { u =>
      gen.writeStringField("duration_unit", u.toString.toLowerCase)
    }
  }

  private[json] def percentileKey(p: Double): String = {
    if (p < 0.0 || p > 1.0) {
      return "invalid"
    }

    val s = (BigDecimal(p) * 1000).setScale(3).rounded.formatted("%03d")
    if (s.endsWith("0")) s"p${s.substring(0, 2)}" else s"p$s"
  }
}

@SerialVersionUID(1L)
private class BoxedDoubleSerializer(pattern: String)
  extends StdSerializer[BoxedDouble](classOf[BoxedDouble]) {

  private val formatter = new DecimalFormat(pattern)

  override def serialize(value: BoxedDouble, gen: JsonGenerator,
                         provider: SerializerProvider): Unit = {
    if (value == null) gen.writeNull()
    else gen.writeNumber(formatter.format(value))
  }
}

@SerialVersionUID(1L)
private class GaugeSerializer(nc: NamingConvention = NamingConvention.dot)
  extends StdSerializer[Gauge](classOf[Gauge]) with Helper {

  override def serialize(gauge: Gauge, gen: JsonGenerator,
                         provider: SerializerProvider): Unit = {
    gen.writeStartObject()
    writeId(gen, nc, gauge.getId)
    gen.writeObjectField("value", gauge.value())
    gen.writeEndObject()
  }
}

@SerialVersionUID(1L)
private class StepCounterSerializer(nc: NamingConvention, step: Duration)
  extends StdSerializer[StepCounter](classOf[StepCounter]) with Helper {

  override def serialize(counter: StepCounter, gen: JsonGenerator,
                         provider: SerializerProvider): Unit = {
    gen.writeStartObject()
    writeId(gen, nc, counter.getId)
    gen.writeObjectField("count", counter.count)
    gen.writeObjectField("rate", counter.count / step.getSeconds)
    gen.writeStringField("rate_unit", "per_second")
    gen.writeEndObject()
  }
}

@SerialVersionUID(1L)
private class StepTimerSerializer(nc: NamingConvention, val step: Duration)
  extends StdSerializer[StepTimer](classOf[StepTimer]) with Helper {

  override def serialize(timer: StepTimer, gen: JsonGenerator,
                         provider: SerializerProvider): Unit = {
    gen.writeStartObject()
    writeId(gen, nc, timer.getId)
    writeHistogramSnapshot(gen, timer.takeSnapshot(), step, Some(TimeUnit.MILLISECONDS))
    gen.writeEndObject()
  }
}

@SerialVersionUID(1L)
private class StepDistributionSummarySerializer(nc: NamingConvention, val step: Duration)
  extends StdSerializer[StepDistributionSummary](classOf[StepDistributionSummary]) with Helper {
  override def serialize(summary: StepDistributionSummary,
                         gen: JsonGenerator,
                         provider: SerializerProvider): Unit = {
    gen.writeStartObject()
    writeId(gen, nc, summary.getId)
    writeHistogramSnapshot(gen, summary.takeSnapshot(), step, None)
    gen.writeEndObject()
  }
}

@SerialVersionUID(1L)
private class LongTaskTimerSerializer(nc: NamingConvention, val step: Duration)
  extends StdSerializer[LongTaskTimer](classOf[LongTaskTimer]) with Helper {

  override def serialize(timer: LongTaskTimer, gen: JsonGenerator,
                         provider: SerializerProvider): Unit = {
    gen.writeStartObject()
    writeId(gen, nc, timer.getId)
    gen.writeNumberField("active", timer.activeTasks())
    gen.writeObjectField("duration", timer.duration(TimeUnit.MILLISECONDS))
    writeHistogramSnapshot(gen, timer.takeSnapshot(), step, Some(TimeUnit.MILLISECONDS))
    gen.writeEndObject()
  }
}

@SerialVersionUID(1L)
private class TimeGaugeCounterSerializer(nc: NamingConvention, val step: Duration)
  extends StdSerializer[TimeGauge](classOf[TimeGauge]) with Helper {

  override def serialize(gauge: TimeGauge, gen: JsonGenerator,
                         provider: SerializerProvider): Unit = {
    gen.writeStartObject()
    writeId(gen, nc, gauge.getId)
    gen.writeObjectField("value", gauge.value(TimeUnit.MILLISECONDS))
    gen.writeStringField("duration_unit", BaseUnits.MILLISECONDS)
    gen.writeEndObject()
  }
}

@SerialVersionUID(1L)
private class StepFunctionCounterSerializer(nc: NamingConvention, val step: Duration)
  extends StdSerializer[StepFunctionCounter[_]](classOf[StepFunctionCounter[_]]) with Helper {

  override def serialize(counter: StepFunctionCounter[_], gen: JsonGenerator,
                         provider: SerializerProvider): Unit = {
    gen.writeStartObject()
    writeId(gen, nc, counter.getId)
    gen.writeObjectField("count", counter.count)
    gen.writeObjectField("rate", counter.count / step.getSeconds)
    gen.writeStringField("rate_unit", "per_second")
    gen.writeEndObject()
  }
}

@SerialVersionUID(1L)
private class StepFunctionTimerSerializer(nc: NamingConvention, val step: Duration)
  extends StdSerializer[StepFunctionTimer[_]](classOf[StepFunctionTimer[_]]) with Helper {

  override def serialize(timer: StepFunctionTimer[_], gen: JsonGenerator,
                         provider: SerializerProvider): Unit = {
    gen.writeStartObject()
    writeId(gen, nc, timer.getId)
    gen.writeObjectField("count", timer.count)
    gen.writeObjectField("rate", timer.count / step.getSeconds)
    gen.writeStringField("rate_unit", "per_second")
    gen.writeObjectField("total", timer.totalTime(TimeUnit.MILLISECONDS))
    gen.writeObjectField("mean", timer.mean(TimeUnit.MILLISECONDS))
    gen.writeStringField("duration_unit", BaseUnits.MILLISECONDS)

    gen.writeEndObject()
  }
}

@SerialVersionUID(1L)
private class MeterSerializer(nc: NamingConvention)
  extends StdSerializer[Meter](classOf[Meter]) with Helper {

  override def serialize(meter: Meter, gen: JsonGenerator,
                         provider: SerializerProvider): Unit = {
    gen.writeStartObject()
    writeId(gen, nc, meter.getId)
    meter.measure().forEach { m: Measurement =>
      gen.writeObjectField(m.getStatistic.getTagValueRepresentation, m.getValue)
    }
    gen.writeEndObject()
  }
}

object MicrometerSerializers {

  def registerSerializers(mapper: ObjectMapper,
                          step: Duration,
                          nc: NamingConvention = NamingConvention.dot): Unit = {
    val module = new SimpleModule("kyuubi-micrometer-module")
    module.addSerializer(new BoxedDoubleSerializer("0.####"))
    module.addSerializer(new GaugeSerializer(nc))
    module.addSerializer(new StepCounterSerializer(nc, step))
    module.addSerializer(new StepTimerSerializer(nc, step))
    module.addSerializer(new LongTaskTimerSerializer(nc, step))
    module.addSerializer(new StepDistributionSummarySerializer(nc, step))
    module.addSerializer(new StepFunctionCounterSerializer(nc, step))
    module.addSerializer(new StepFunctionTimerSerializer(nc, step))
    module.addSerializer(new MeterSerializer(nc))
    mapper.registerModule(module)
  }
}
