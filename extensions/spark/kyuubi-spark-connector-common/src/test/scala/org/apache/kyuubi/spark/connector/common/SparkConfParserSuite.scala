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

package org.apache.kyuubi.spark.connector.common

import scala.collection.JavaConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class SparkConfParserSuite extends SparkFunSuite {

  private var confParser: SparkConfParser = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val options = new CaseInsensitiveStringMap(Map(
      "key1" -> "111",
      "booleanKey" -> "false",
      "intKey" -> "10",
      "longKey" -> String.valueOf(Long.MaxValue),
      "doubleKey" -> "1.1",
      "bytesKey" -> "1k",
      "timeKey" -> "1s").asJava)
    val properties = Map(
      "key1" -> "333",
      "propertyKey1" -> "propertyValue1")
    confParser = SparkConfParser(options, null, properties.asJava)
  }

  test("parse options config") {
    assert(confParser.stringConf().option("optKey1").defaultStringValue("test").parse() === "test")
    assert(
      confParser.booleanConf().option("booleanKey").defaultStringValue("true").parse() === false)
    assert(confParser.intConf().option("intKey").defaultStringValue("0").parse() === 10)
    assert(
      confParser.longConf().option("longKey").defaultStringValue("0").parse() === Long.MaxValue)
    assert(confParser.doubleConf().option("doubleKey").defaultStringValue("0.0").parse() === 1.1)
    assert(confParser.bytesConf().option("bytesKey").defaultStringValue("0k").parse() === 1024L)
    assert(confParser.timeConf().option("timeKey").defaultStringValue("0s").parse() === 1000L)
  }

  test("parse properties config") {
    assert(confParser.intConf().option("key1")
      .tableProperty("key1")
      .defaultStringValue("0").parse() === 111)
    assert(confParser.stringConf()
      .option("propertyKey1")
      .tableProperty("propertyKey1")
      .defaultStringValue("test").parse() === "propertyValue1")
  }
}
