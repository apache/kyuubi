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
import io.micrometer.core.instrument.config.NamingConvention
import io.micrometer.core.instrument.distribution.ValueAtPercentile

trait Helper {

  def writeId(gen: JsonGenerator, nc: NamingConvention, id: Meter.Id): Unit = {
    gen.writeStringField("name", id.getConventionName(nc))
    gen.writeObjectFieldStart("tags")
    id.getConventionTags(nc).forEach { t: Tag => gen.writeStringField(t.getKey, t.getValue) }
    gen.writeEndObject()
    gen.writeStringField("base_unit", id.getBaseUnit)
    gen.writeStringField("type", id.getType.name().toLowerCase)
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
private class GaugeSerializer(nc: NamingConvention = NamingConvention.dot)
  extends StdSerializer[Gauge](classOf[Gauge]) with Helper {

  override def serialize(gauge: Gauge, gen: JsonGenerator,
                         provider: SerializerProvider): Unit = {
    gen.writeStartObject()
    writeId(gen, nc, gauge.getId)
    val v: Double = gauge.value()
    gen.writeObjectField("value", v)
    gen.writeEndObject()
  }
}

@SerialVersionUID(1L)
private class CounterSerializer(nc: NamingConvention = NamingConvention.dot,
                                step: Duration)
  extends StdSerializer[Counter](classOf[Counter]) with Helper {

  override def serialize(counter: Counter, gen: JsonGenerator,
                         provider: SerializerProvider): Unit = {
    gen.writeStartObject()
    writeId(gen, nc, counter.getId)
    gen.writeObjectField("count", counter.count)
    gen.writeObjectField("rate", counter.count / step.getSeconds)
    gen.writeEndObject()
  }
}

@SerialVersionUID(1L)
private class TimerSerializer(nc: NamingConvention = NamingConvention.dot,
                              val step: Duration)
  extends StdSerializer[Timer](classOf[Timer]) with Helper {

  final val rateUnit: TimeUnit = TimeUnit.SECONDS
  final val durationUnit: TimeUnit = TimeUnit.MILLISECONDS

  override def serialize(timer: Timer, gen: JsonGenerator,
                         provider: SerializerProvider): Unit = {
    gen.writeStartObject()
    writeId(gen, nc, timer.getId)
    val snapshot = timer.takeSnapshot()
    gen.writeObjectField("count", snapshot.count)
    gen.writeObjectField("max", snapshot.max(durationUnit))
    gen.writeObjectField("mean", snapshot.mean(durationUnit))
    gen.writeObjectField("total", snapshot.total(durationUnit))

    // write percentile, i.e. p50, p75, p95, p99, p999
    snapshot.percentileValues().foreach { p: ValueAtPercentile =>
      gen.writeObjectField(percentileKey(p.percentile), p.value(durationUnit))
    }

    gen.writeObjectField("rate", snapshot.total() / step.getSeconds)
    gen.writeStringField("duration_unit", "ms")
    gen.writeStringField("rate_unit", "tps")

    gen.writeEndObject()
  }
}

@SerialVersionUID(1L)
private class DistributionSummarySerializer(nc: NamingConvention = NamingConvention.dot,
                                            val step: Duration)
  extends StdSerializer[DistributionSummary](classOf[DistributionSummary]) with Helper {
  override def serialize(summary: DistributionSummary,
                         gen: JsonGenerator,
                         provider: SerializerProvider): Unit = {
    val snapshot = summary.takeSnapshot()
    gen.writeStartObject()
    writeId(gen, nc, summary.getId)
    gen.writeObjectField("count", snapshot.count)
    gen.writeObjectField("max", snapshot.max)
    gen.writeObjectField("mean", snapshot.mean)
    gen.writeObjectField("total", snapshot.total)

    // write percentile, i.e. p50, p75, p95, p99, p999
    snapshot.percentileValues().foreach { p: ValueAtPercentile =>
      gen.writeObjectField(percentileKey(p.percentile), p.value)
    }

    gen.writeObjectField("rate", snapshot.total() / step.getSeconds)
    gen.writeStringField("duration_unit", "ms")
    gen.writeStringField("rate_unit", "tps")

    gen.writeEndObject()
  }
}

@SerialVersionUID(1L)
private class MeterSerializer(nc: NamingConvention = NamingConvention.dot)
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

@SerialVersionUID(1L)
private class BoxedDoubleSerializer(pattern: String)
  extends StdSerializer[BoxedDouble](classOf[BoxedDouble]) {

  private val formatter = new DecimalFormat(pattern)

  override def serialize(value: BoxedDouble, gen: JsonGenerator,
                         provider: SerializerProvider): Unit = {
    if (value == null) gen.writeNull() else gen.writeNumber(formatter.format(value))
  }
}

object MicrometerSerializers {

  def registerSerializers(mapper: ObjectMapper,
                          step: Duration,
                          nc: NamingConvention = NamingConvention.dot): Unit = {
    val module = new SimpleModule("kyuubi-micrometer-module")
    module.addSerializer(new GaugeSerializer(nc))
    module.addSerializer(new CounterSerializer(nc, step))
    module.addSerializer(new TimerSerializer(nc, step))
    module.addSerializer(new DistributionSummarySerializer(nc, step))
    module.addSerializer(new MeterSerializer(nc))
    module.addSerializer(new BoxedDoubleSerializer("0.####"))
    mapper.registerModule(module)
  }
}
