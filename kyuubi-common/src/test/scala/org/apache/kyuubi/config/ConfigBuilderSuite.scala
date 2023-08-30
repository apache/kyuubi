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

package org.apache.kyuubi.config

import java.time.Duration

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.util.AssertionUtils._

class ConfigBuilderSuite extends KyuubiFunSuite {

  test("int config") {
    val intConf = ConfigBuilder("kyuubi.int.conf")
      .intConf
      .createWithDefault(0)
    assert(intConf.key === "kyuubi.int.conf")
    assert(intConf.defaultVal.get === 0)
  }

  test("long config") {
    val longConf = ConfigBuilder("kyuubi.long.conf")
      .longConf
      .createWithDefault(1)
    assert(longConf.key === "kyuubi.long.conf")
    assert(longConf.defaultVal.get === 1)
  }

  test("double config") {
    val doubleConf = ConfigBuilder("kyuubi.double.conf")
      .doubleConf
      .createWithDefault(2.0)
    assert(doubleConf.key === "kyuubi.double.conf")
    assert(doubleConf.defaultVal.get === 2.0)
  }

  test("boolean config") {
    val booleanConf = ConfigBuilder("kyuubi.boolean.conf")
      .booleanConf
      .createWithDefault(false)
    assert(booleanConf.key === "kyuubi.boolean.conf")
    assert(!booleanConf.defaultVal.get)

    KyuubiConf.register(booleanConf)
    val kyuubiConf = KyuubiConf().set(booleanConf.key, "invalid conf")
    val e = intercept[IllegalArgumentException](kyuubiConf.get(booleanConf))
    assert(e.getMessage === "kyuubi.boolean.conf should be boolean, but was invalid conf")
  }

  test("string config") {
    val stringConf = ConfigBuilder("kyuubi.string.conf")
      .stringConf
      .createWithDefault("kent, yao")
    assert(stringConf.key === "kyuubi.string.conf")
    assert(stringConf.defaultVal.get === "kent, yao")
    val sequenceConf = ConfigBuilder("kyuubi.sequence.conf")
      .stringConf
      .toSequence()
      .createWithDefault(Nil)
    assert(sequenceConf.defaultVal.get.isEmpty)
    KyuubiConf.register(sequenceConf)
    val kyuubiConf = KyuubiConf().set(sequenceConf.key, "kyuubi,kent")
    assert(kyuubiConf.get(sequenceConf) === Seq("kyuubi", "kent"))

    val stringConfUpper = ConfigBuilder("kyuubi.string.conf.upper")
      .stringConf
      .transformToUpperCase
      .createWithDefault("Kent, Yao")
    assert(stringConfUpper.key === "kyuubi.string.conf.upper")
    assert(stringConfUpper.defaultVal.get === "KENT, YAO")

    val stringConfUpperSeq = ConfigBuilder("kyuubi.string.conf.upper.seq")
      .stringConf
      .transformToUpperCase
      .toSequence()
      .createWithDefault(Seq("hehe"))
    assert(stringConfUpperSeq.defaultVal.get === Seq("HEHE"))

    val stringConfSet = ConfigBuilder("kyuubi.string.conf.set")
      .stringConf
      .toSet()
      .createWithDefault(Set("hehe", "haha"))
    assert(stringConfSet.defaultVal.get === Set("hehe", "haha"))

    val stringConfLower = ConfigBuilder("kyuubi.string.conf.lower")
      .stringConf
      .transformToLowerCase
      .createWithDefault("Kent, Yao")
    assert(stringConfLower.key === "kyuubi.string.conf.lower")
    assert(stringConfLower.defaultVal.get === "kent, yao")
  }

  test("time config") {
    val timeConf = ConfigBuilder("kyuubi.time.config")
      .timeConf
      .createWithDefault(3)
    assert(timeConf.key === "kyuubi.time.config")
    assert(timeConf.defaultVal.get === 3)
    val kyuubiConf = KyuubiConf().set(timeConf.key, "invalid")
    KyuubiConf.register(timeConf)
    val e = intercept[IllegalArgumentException](kyuubiConf.get(timeConf))
    assert(e.getMessage startsWith "The formats accepted are 1) based on the ISO-8601")
  }

  test("invalid config with negative value") {
    val intConf = ConfigBuilder("kyuubi.invalid.config")
      .intConf
      .checkPositive
      .createWithDefault(3)
    assert(intConf.key === "kyuubi.invalid.config")
    assert(intConf.defaultVal.get === 3)
    val kyuubiConf = KyuubiConf().set(intConf.key, "-1")
    KyuubiConf.register(intConf)
    interceptEquals[IllegalArgumentException](kyuubiConf.get(intConf))(
      "'-1' in kyuubi.invalid.config is invalid. Must be a positive Integer.")
  }

  test("invalid config with non-negative value") {
    val longConf = ConfigBuilder("kyuubi.invalid.config.nonnegative")
      .longConf
      .checkNonNegative
      .createWithDefault(3)
    assert(longConf.key === "kyuubi.invalid.config.nonnegative")
    assert(longConf.defaultVal.get === 3)
    val kyuubiConf = KyuubiConf().set(longConf.key, "-1")
    KyuubiConf.register(longConf)
    interceptEquals[IllegalArgumentException](kyuubiConf.get(longConf))(
      "'-1' in kyuubi.invalid.config.nonnegative is invalid. Must be a non-negative Long.")
  }

  test("invalid config out of range") {
    val key1 = "kyuubi.invalid.config.range.exclusive"
    val inRangeConf1 = ConfigBuilder(key1)
      .longConf
      .checkRange(1 until 10)
      .createWithDefault(1)
    assert(inRangeConf1.key === key1)
    assert(inRangeConf1.defaultVal.get === 1)
    val kyuubiConf1 = KyuubiConf().set(inRangeConf1.key, "10")
    KyuubiConf.register(inRangeConf1)
    interceptEquals[IllegalArgumentException](kyuubiConf1.get(inRangeConf1))(
      "'10' in kyuubi.invalid.config.range.exclusive is invalid." +
        " Must be a Long in the range of [1, 10).")

    val key2 = "kyuubi.invalid.config.range.inclusive"
    val inRangeConf2 = ConfigBuilder(key2)
      .longConf
      .checkRange(1 to 10)
      .createWithDefault(10)
    assert(inRangeConf2.key === key2)
    assert(inRangeConf2.defaultVal.get === 10)
    val kyuubiConf2 = KyuubiConf().set(inRangeConf2.key, "0")
    KyuubiConf.register(inRangeConf2)
    interceptEquals[IllegalArgumentException](kyuubiConf2.get(inRangeConf2))(
      "'0' in kyuubi.invalid.config.range.inclusive is invalid." +
        " Must be a Long in the range of [1, 10].")
  }

  test("invalid config out of duration range") {
    val key1 = "kyuubi.invalid.config.duration.min"
    val inRangeConf1 = ConfigBuilder(key1)
      .timeConf
      .checkDurationRange(min = Some(Duration.ofSeconds(2)))
      .createWithDefault(Duration.ofSeconds(2).toMillis)
    assert(inRangeConf1.key === key1)
    assert(inRangeConf1.defaultVal.get === 2000)
    val kyuubiConf1 = KyuubiConf().set(inRangeConf1.key, "PT1S")
    KyuubiConf.register(inRangeConf1)
    interceptEquals[IllegalArgumentException](kyuubiConf1.get(inRangeConf1))(
      "'1000' in kyuubi.invalid.config.duration.min is invalid." +
        " Must be a duration equals or greater than 2000ms.")

    val key2 = "kyuubi.invalid.config.duration.max"
    val inRangeConf2 = ConfigBuilder(key2)
      .timeConf
      .checkDurationRange(max = Some(Duration.ofSeconds(5)))
      .createWithDefault(Duration.ofSeconds(4).toMillis)
    assert(inRangeConf2.key === key2)
    assert(inRangeConf2.defaultVal.get === 4000)
    val kyuubiConf2 = KyuubiConf().set(inRangeConf2.key, "PT6S")
    KyuubiConf.register(inRangeConf2)
    interceptEquals[IllegalArgumentException](kyuubiConf2.get(inRangeConf2))(
      "'6000' in kyuubi.invalid.config.duration.max is invalid." +
        " Must be a duration equals or less than 5000ms.")

    val key3 = "kyuubi.invalid.config.duration.between"
    val inRangeConf3 = ConfigBuilder(key3)
      .timeConf
      .checkDurationRange(min = Some(Duration.ofSeconds(1)), max = Some(Duration.ofSeconds(5)))
      .createWithDefault(Duration.ofSeconds(4).toMillis)
    assert(inRangeConf3.key === key3)
    assert(inRangeConf3.defaultVal.get === 4000)
    val kyuubiConf3 = KyuubiConf().set(inRangeConf3.key, "PT6S")
    KyuubiConf.register(inRangeConf3)
    interceptEquals[IllegalArgumentException](kyuubiConf3.get(inRangeConf3))(
      "'6000' in kyuubi.invalid.config.duration.between is invalid." +
        " Must be a duration between 1000ms and 5000ms.")
  }

  test("invalid config for enum") {
    object TempEnum extends Enumeration {
      type TempEnum = Value
      val ValA, ValB = Value
    }
    val stringConf = ConfigBuilder("kyuubi.invalid.config.enum")
      .stringConf
      .checkValues(TempEnum)
      .createWithDefault("ValA")
    assert(stringConf.key === "kyuubi.invalid.config.enum")
    assert(stringConf.defaultVal.get === "ValA")
    val kyuubiConf = KyuubiConf().set(stringConf.key, "ValC")
    KyuubiConf.register(stringConf)
    interceptEquals[IllegalArgumentException] { kyuubiConf.get(stringConf) }(
      "The value of kyuubi.invalid.config.enum should be one of ValA, ValB, but was ValC")
  }
}
