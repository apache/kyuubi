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

import scala.util.{Failure, Success, Try}

private[kyuubi] case class ConfigBuilder(key: String) {

  private[config] var _doc = ""
  private[config] var _version = ""
  private[config] var _onCreate: Option[ConfigEntry[_] => Unit] = None
  private[config] var _type = ""

  def doc(s: String): ConfigBuilder = {
    _doc = s
    this
  }

  def version(s: String): ConfigBuilder = {
    _version = s
    this
  }

  def onCreate(callback: ConfigEntry[_] => Unit): ConfigBuilder = {
    _onCreate = Option(callback)
    this
  }

  private def toNumber[T](s: String, converter: String => T): T = {
    try {
      converter(s.trim)
    } catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(s"$key should be ${_type}, but was $s")
    }
  }

  def intConf: TypedConfigBuilder[Int] = {
    _type = "int"
    new TypedConfigBuilder(this, toNumber(_, _.toInt))
  }

  def longConf: TypedConfigBuilder[Long] = {
    _type = "long"
    new TypedConfigBuilder(this, toNumber(_, _.toLong))
  }

  def doubleConf: TypedConfigBuilder[Double] = {
    _type = "double"
    new TypedConfigBuilder(this, toNumber(_, _.toDouble))
  }

  def booleanConf: TypedConfigBuilder[Boolean] = {
    _type = "boolean"
    def toBoolean(s: String) = try {
      s.trim.toBoolean
    } catch {
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$key should be boolean, but was $s", e)
    }
    new TypedConfigBuilder(this, toBoolean)
  }

  def stringConf: TypedConfigBuilder[String] = {
    _type = "string"
    new TypedConfigBuilder(this, identity)
  }

  def timeConf: TypedConfigBuilder[Long] = {
    _type = "duration"
    def timeFromStr(str: String): Long = {
      val trimmed = str.trim
      Try(Duration.parse(trimmed).toMillis)
        .orElse(Try(trimmed.toLong)) match {
        case Success(millis) => millis
        case Failure(e) =>
          throw new IllegalArgumentException(s"The formats accepted are 1) based on the ISO-8601" +
            s" duration format `PnDTnHnMn.nS` with days considered to be exactly 24 hours. 2) A" +
            s" plain long value represents total milliseconds, e.g. 2000 means 2 seconds" +
            s" $trimmed for $key is not valid", e)
      }
    }

    def timeToStr(v: Long): String = {
      Duration.ofMillis(v).toString
    }

    new TypedConfigBuilder[Long](this, timeFromStr, timeToStr)
  }

  def fallbackConf[T](fallback: ConfigEntry[T]): ConfigEntry[T] = {
    val entry =
      new ConfigEntryFallback[T](key, _doc, _version, fallback)
    _onCreate.foreach(_(entry))
    entry
  }
}

private[kyuubi] case class TypedConfigBuilder[T](
    parent: ConfigBuilder,
    fromStr: String => T,
    toStr: T => String) {

  import ConfigHelpers._

  def this(parent: ConfigBuilder, fromStr: String => T) =
    this(parent, fromStr, Option[T](_).map(_.toString).orNull)

  def transform(fn: T => T): TypedConfigBuilder[T] = this.copy(fromStr = s => fn(fromStr(s)))

  /** Checks if the user-provided value for the config matches the validator. */
  def checkValue(validator: T => Boolean, errMsg: String): TypedConfigBuilder[T] = {
    transform { v =>
      if (!validator(v)) throw new IllegalArgumentException(errMsg)
      v
    }
  }

  /** Check that user-provided values for the config match a pre-defined set. */
  def checkValues(validValues: Set[T]): TypedConfigBuilder[T] = {
    transform { v =>
      if (!validValues.contains(v)) {
        throw new IllegalArgumentException(
          s"The value of ${parent.key} should be one of ${validValues.mkString(", ")}, but was $v")
      }
      v
    }
  }

  /** Turns the config entry into a sequence of values of the underlying type. */
  def toSequence(sp: String = ","): TypedConfigBuilder[Seq[T]] = {
    parent._type = "seq"
    TypedConfigBuilder(parent, strToSeq(_, fromStr, sp), seqToStr(_, toStr))
  }

  def createOptional: OptionalConfigEntry[T] = {
    val entry = new OptionalConfigEntry(
      parent.key, fromStr, toStr, parent._doc, parent._version, parent._type)
    parent._onCreate.foreach(_(entry))
    entry
  }

  def createWithDefault(default: T): ConfigEntry[T] = default match {
    case d: String => createWithDefaultString(d)
    case _ =>
      val d = fromStr(toStr(default))
      val entry = new ConfigEntryWithDefault(
        parent.key, d, fromStr, toStr, parent._doc, parent._version, parent._type)
      parent._onCreate.foreach(_(entry))
      entry
  }

  def createWithDefaultString(default: String): ConfigEntryWithDefaultString[T] = {
    val entry = new ConfigEntryWithDefaultString(
      parent.key, default, fromStr, toStr, parent._doc, parent._version, parent._type)
    parent._onCreate.foreach(_(entry))
    entry
  }
}
