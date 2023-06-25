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

import java.util

import org.apache.spark.sql.RuntimeConfig

/**
 * Parse the value of configuration in runtime options, session configurations and table properties,
 * the priority of parsing configuration: options > sessionConfigs > properties.
 * @param options runtime options
 * @param sessionConfigs spark session configurations
 * @param properties table properties
 */
case class SparkConfParser(
    options: util.Map[String, String],
    sessionConfigs: RuntimeConfig,
    properties: util.Map[String, String]) {

  def booleanConf(): BooleanConfParser = new BooleanConfParser()
  def intConf(): IntConfParser = new IntConfParser()
  def longConf(): LongConfParser = new LongConfParser()
  def doubleConf(): DoubleConfParser = new DoubleConfParser()
  def stringConf(): StringConfParser = new StringConfParser()
  def bytesConf(): BytesConfParser = new BytesConfParser()
  def timeConf(): TimeConfParser = new TimeConfParser()

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

  class BytesConfParser extends ConfParser[Long] {
    override protected def conversion(value: String): Long = JavaUtils.byteStringAsBytes(value)
  }

  class TimeConfParser extends ConfParser[Long] {
    override protected def conversion(value: String): Long = JavaUtils.timeStringAsMs(value)
  }

  abstract class ConfParser[T]() {
    private var optionName: Option[String] = None
    private var sessionConfName: Option[String] = None
    private var tablePropertyName: Option[String] = None
    private var defaultStringValue: Option[String] = None

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

    def defaultStringValue(value: String): ConfParser[T] = {
      this.defaultStringValue = Some(value)
      this
    }

    private def parse(conversion: String => T): Option[T] = {
      var valueOpt: Option[String] = None
      if (options != null) {
        valueOpt = optionName.flatMap(name => Option(options.get(name)))
      }
      if (valueOpt.isEmpty && sessionConfigs != null) {
        valueOpt = sessionConfName.flatMap(name => sessionConfigs.getOption(name))
      }
      if (valueOpt.isEmpty && properties != null) {
        valueOpt = tablePropertyName.flatMap(name => Option(properties.get(name)))
      }
      valueOpt.orElse(defaultStringValue).map(conversion)
    }

    protected def conversion(value: String): T

    def parse(): T = {
      assert(defaultStringValue.isDefined, "Default value cannot be empty.")
      parseOptional().get
    }

    def parseOptional(): Option[T] = parse(conversion)
  }
}
