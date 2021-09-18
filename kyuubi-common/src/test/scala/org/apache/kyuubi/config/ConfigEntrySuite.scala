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

class ConfigEntrySuite extends KyuubiFunSuite {

  test("optional config entry") {
    val doc = "this is dummy documentation"
    val e1 = new OptionalConfigEntry[Int](
      "kyuubi.int.spark",
      s => s.toInt + 1,
      v => (v - 1).toString,
      doc,
      "<none>",
      "int",
      false)

    assert(e1.key === "kyuubi.int.spark")
    assert(e1.valueConverter("2") === Some(3))
    assert(e1.strConverter(Some(1)) === "0")
    assert(e1.defaultValStr === ConfigEntry.UNDEFINED)
    assert(e1.defaultVal.isEmpty)
    assert(e1.doc === doc)
    assert(e1.version === "<none>")
    assert(e1.typ === "int")
    assert(e1.internal === false)
    assert(e1.toString === s"ConfigEntry(key=kyuubi.int.spark, defaultValue=<undefined>," +
      s" doc=$doc, version=<none>, type=int)")

    val conf = KyuubiConf()
    assert(conf.get(e1).isEmpty)
    val e = intercept[IllegalArgumentException](new OptionalConfigEntry[Int](
      "kyuubi.int.spark",
      s => s.toInt + 1,
      v => (v - 1).toString,
      "this is dummy documentation",
      "<none>",
      "int",
      false))
    assert(e.getMessage ===
      "requirement failed: Config entry kyuubi.int.spark already registered!")
    conf.set(e1.key, "2")
    assert(conf.get(e1) === Some(3))

  }

  test("config entry with default") {
    val e1 = new ConfigEntryWithDefault[Long]("kyuubi.long.spark",
      2,
      s => s.toLong + 1,
      v => (v - 1).toString,
    "doc",
    "0.11.1",
      "long",
      false)

    assert(e1.key === "kyuubi.long.spark")
    assert(e1.valueConverter("2") === 3)
    assert(e1.strConverter(1) === "0")
    assert(e1.defaultValStr === "1")
    assert(e1.defaultVal === Some(2))
    assert(e1.doc === "doc")
    assert(e1.version === "0.11.1")
    assert(e1.typ === "long")
    assert(e1.internal === false)
    assert(e1.toString === s"ConfigEntry(key=kyuubi.long.spark, defaultValue=1," +
      s" doc=doc, version=0.11.1, type=long)")

    val conf = KyuubiConf()
    assert(conf.get(e1) === 2)
    conf.set(e1.key, "5")
    assert(conf.get(e1) === 6)
  }

  test("config entry with default string") {
    val e1 = new ConfigEntryWithDefaultString[Double](
      "kyuubi.double.spark",
      "3.0",
      s => java.lang.Double.valueOf(s),
      v => v.toString,
      "doc",
      "",
      "double",
      false)

    assert(e1.key === "kyuubi.double.spark")
    assert(e1.valueConverter("2") === 2.0)
    assert(e1.strConverter(1) === "1.0")
    assert(e1.defaultValStr === "3.0")
    assert(e1.defaultVal === Some(3.0))
    assert(e1.doc === "doc")
    assert(e1.version === "")
    assert(e1.typ === "double")
    assert(e1.internal === false)
    assert(e1.toString === s"ConfigEntry(key=kyuubi.double.spark, defaultValue=3.0," +
      s" doc=doc, version=, type=double)")

    val conf = KyuubiConf()
    assert(conf.get(e1) === 3.0)
    conf.set(e1.asInstanceOf[ConfigEntry[AnyVal]], 5.0)
    assert(conf.get(e1) === 5.0)
  }

  test("fallback config entry") {
    val origin = KyuubiConf.buildConf("origin.spark")
      .version("1.1.1")
      .stringConf.createWithDefault("origin")
    val fallback =
      new ConfigEntryFallback[String]("kyuubi.fallback.spark", "fallback", "1.2.0", false, origin)

    assert(fallback.key === "kyuubi.fallback.spark")
    assert(fallback.valueConverter("2") === "2")
    assert(fallback.strConverter("2.0") === "2.0")
    assert(fallback.defaultValStr === "origin")
    assert(fallback.defaultVal === Some("origin"))
    assert(fallback.doc === "fallback")
    assert(fallback.version === "1.2.0")
    assert(fallback.typ === "string")
    assert(fallback.internal === false)
    assert(fallback.toString === s"ConfigEntry(key=kyuubi.fallback.spark, defaultValue=origin," +
      s" doc=fallback, version=1.2.0, type=string)")

    val conf = KyuubiConf()
    assert(conf.get(fallback) === "origin")
    conf.set(origin.key, "new value")
    assert(conf.get(fallback) === "new value", "fallback to new original key")
  }
}
