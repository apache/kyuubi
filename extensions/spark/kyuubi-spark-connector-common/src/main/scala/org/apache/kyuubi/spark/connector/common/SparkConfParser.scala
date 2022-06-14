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

import java.util.Locale

import org.apache.spark.sql.RuntimeConfig

case class SparkConfParser(
    options: Map[String, String],
    sessionConfigs: RuntimeConfig,
    properties: Map[String, String]) {

  def booleanConf(): BooleanConfParser = new BooleanConfParser()
  def intConf(): IntConfParser = new IntConfParser()
  def longConf(): LongConfParser = new LongConfParser()
  def doubleConf(): DoubleConfParser = new DoubleConfParser()
  def stringConf(): StringConfParser = new StringConfParser()

  class BooleanConfParser extends ConfParser[Boolean] {
    override protected def conversion(value: String): Boolean = value.toBoolean
  }

  class IntConfParser extends ConfParser[Int] {
    override protected def conversion(value: String): Int = value.toInt
  }

  class LongConfParser extends ConfParser[Long] {
    override protected def conversion(value: String): Long = value.toLong
  }

  class DoubleConfParser extends ConfParser[Double] {
    override protected def conversion(value: String): Double = value.toDouble
  }

  class StringConfParser extends ConfParser[String] {
    override protected def conversion(value: String): String = value
  }

  abstract class ConfParser[T]() {
    private var optionName: Option[String] = None
    private var sessionConfName: Option[String] = None
    private var tablePropertyName: Option[String] = None
    private var defaultValue: Option[T] = None

    def option(name: String): ConfParser[T] = {
      this.optionName = Some(name)
      this
    }

    def sessionConf(name: String): ConfParser[T] = {
      this.sessionConfName = Some(name)
      this
    }

    def tableProperty(name: String): ConfParser[T] = {
      this.tablePropertyName = Some(name)
      this
    }

    def defaultValue(value: T): ConfParser[T] = {
      this.defaultValue = Some(value)
      this
    }

    private def parse(conversion: String => T): Option[T] = {
      // use lower case comparison as DataSourceOptions.asMap() in Spark 2 returns a lower case map
      var valueOpt: Option[String] = None
      if (options != null) {
        valueOpt = optionName.flatMap(name => options.get(name.toLowerCase(Locale.ROOT)))
      }
      if (valueOpt.isEmpty && sessionConfigs != null) {
        valueOpt = sessionConfName.flatMap(name => sessionConfigs.getOption(name))
      }
      if (valueOpt.isEmpty && properties != null) {
        valueOpt = tablePropertyName.flatMap(name => properties.get(name))
      }
      valueOpt = valueOpt.filter(_ != null)
      if (valueOpt.isDefined) {
        valueOpt.map(conversion(_))
      } else {
        defaultValue
      }
    }

    protected def conversion(value: String): T

    def parse(): T = {
      assert(defaultValue.isDefined, "Default value cannot be empty.")
      parseOptional.get
    }

    def parseOptional(): Option[T] = parse(conversion)
  }
}
