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

// scalastyle:off anyfunsuite
import scala.collection.JavaConverters._

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class SparkConfParserSuite extends AnyFunSuite with BeforeAndAfterAll {
// scalastyle:on anyfunsuite

  test("parse options config") {
    assert(confParser.stringConf().option("optKey1").defaultValue("test").parse() === "optValue1")
    assert(confParser.booleanConf().option("booleanKey").defaultValue(true).parse() === false)
    assert(confParser.intConf().option("intKey").defaultValue(0).parse() === 10)
    assert(confParser.longConf().option("longKey").defaultValue(0).parse() === Long.MaxValue)
    assert(confParser.doubleConf().option("doubleKey").defaultValue(0.0).parse() === 1.1)
  }

  test("parse properties config") {
    assert(confParser.intConf().option("key1")
      .tableProperty("key1")
      .defaultValue(0).parse() === 111)
    assert(confParser.stringConf()
      .option("propertyKey1")
      .tableProperty("propertyKey1")
      .defaultValue("test").parse() === "propertyValue1")
  }

  private var confParser: SparkConfParser = null

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val options = new CaseInsensitiveStringMap(Map(
      "key1" -> "111",
      "optKey1" -> "optValue1",
      "booleanKey" -> "false",
      "intKey" -> "10",
      "longKey" -> String.valueOf(Long.MaxValue),
      "doubleKey" -> "1.1").asJava)
    val properties = Map(
      "key1" -> "333",
      "propertyKey1" -> "propertyValue1")
    confParser = SparkConfParser(options, null, properties.asJava)
  }

}
