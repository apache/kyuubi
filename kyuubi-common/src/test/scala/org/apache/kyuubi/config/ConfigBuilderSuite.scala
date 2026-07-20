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

  test("invalid config") {
    val intConf = ConfigBuilder("kyuubi.invalid.config")
      .intConf
      .checkValue(t => t > 0, "must be positive integer")
      .createWithDefault(3)
    assert(intConf.key === "kyuubi.invalid.config")
    assert(intConf.defaultVal.get === 3)
    val kyuubiConf = KyuubiConf().set(intConf.key, "-1")
    KyuubiConf.register(intConf)
    val e = intercept[IllegalArgumentException](kyuubiConf.get(intConf))
    assert(e.getMessage equals "'-1' in kyuubi.invalid.config is invalid. must be positive integer")
  }

  test("audience config") {
    val serverConf = ConfigBuilder("kyuubi.audience.server.conf")
      .audience(ConfigAudience.SERVER)
      .stringConf
      .createWithDefault("server-value")
    assert(serverConf.audience === Some(Set(ConfigAudience.SERVER)))
    assert(serverConf.immutable === false)

    val sparkConf = ConfigBuilder("kyuubi.audience.spark.conf")
      .audience(ConfigAudience.SPARK, ConfigAudience.FLINK)
      .stringConf
      .createWithDefault("multi-engine")
    assert(sparkConf.audience === Some(Set(ConfigAudience.SPARK, ConfigAudience.FLINK)))

    val defaultConf = ConfigBuilder("kyuubi.audience.default.conf")
      .stringConf
      .createWithDefault("default")
    assert(defaultConf.audience === None)
  }

  test("immutable config") {
    val immutableConf = ConfigBuilder("kyuubi.immutable.conf")
      .immutable
      .stringConf
      .createWithDefault("locked")
    assert(immutableConf.immutable === true)
    assert(immutableConf.audience === None)

    val mutableConf = ConfigBuilder("kyuubi.mutable.conf")
      .stringConf
      .createWithDefault("unlocked")
    assert(mutableConf.immutable === false)
  }

  test("audience and immutable combined") {
    val conf = ConfigBuilder("kyuubi.combined.conf")
      .audience(ConfigAudience.SERVER)
      .immutable
      .stringConf
      .createWithDefault("secure")
    assert(conf.audience === Some(Set(ConfigAudience.SERVER)))
    assert(conf.immutable === true)
  }

  test("toSequence and toSet preserve custom separator in strConverter") {
    val semicolonSeq = ConfigBuilder("kyuubi.semicolon.seq.conf")
      .stringConf
      .toSequence(";")
      .createWithDefault(Nil)
    KyuubiConf.register(semicolonSeq)
    try {
      val kyuubiConf = KyuubiConf().set(semicolonSeq.key, "a;b;c")
      val value = kyuubiConf.get(semicolonSeq)
      assert(value === Seq("a", "b", "c"))
      assert(semicolonSeq.strConverter(value) === "a;b;c")
    } finally {
      KyuubiConf.unregister(semicolonSeq)
    }

    val colonSet = ConfigBuilder("kyuubi.colon.set.conf")
      .stringConf
      .toSet(":")
      .createWithDefault(Set.empty)
    KyuubiConf.register(colonSet)
    try {
      val kyuubiConf = KyuubiConf().set(colonSet.key, "a:b:c")
      val value = kyuubiConf.get(colonSet)
      assert(value === Set("a", "b", "c"))
      assert(colonSet.strConverter(value) === "a:b:c")
    } finally {
      KyuubiConf.unregister(colonSet)
    }
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
