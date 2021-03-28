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

trait ConfigEntry[T] {
  def key: String
  def valueConverter: String => T
  def strConverter: T => String
  def doc: String
  def version: String
  def typ: String

  def defaultValStr: String
  def defaultVal: Option[T]

  override def toString: String = {
    s"ConfigEntry(key=$key, defaultValue=$defaultValStr, doc=$doc, version=$version, type=$typ)"
  }

  final protected def readString(provider: ConfigProvider): Option[String] = {
    provider.get(key)
  }

  def readFrom(conf: ConfigProvider): T

  ConfigEntry.registerEntry(this)
}

class OptionalConfigEntry[T](
    _key: String,
    rawValueConverter: String => T,
    rawStrConverter: T => String,
    _doc: String,
    _version: String,
    _type: String) extends ConfigEntry[Option[T]] {
  override def valueConverter: String => Option[T] = {
    s => Option(rawValueConverter(s))
  }

  override def strConverter: Option[T] => String = {
    v => v.map(rawStrConverter).orNull
  }

  override def defaultValStr: String = {
    ConfigEntry.UNDEFINED
  }

  override def readFrom(conf: ConfigProvider): Option[T] = {
    readString(conf).map(rawValueConverter)
  }

  override def defaultVal: Option[Option[T]] = None

  override def key: String = _key

  override def doc: String = _doc

  override def version: String = _version

  override def typ: String = _type
}

class ConfigEntryWithDefault[T](
    _key: String,
    _defaultVal: T,
    _valueConverter: String => T,
    _strConverter: T => String,
    _doc: String,
    _version: String,
    _type: String) extends ConfigEntry[T] {
  override def defaultValStr: String = strConverter(_defaultVal)

  override def defaultVal: Option[T] = Option(_defaultVal)

  override def readFrom(conf: ConfigProvider): T = {
    readString(conf).map(valueConverter).getOrElse(_defaultVal)
  }

  override def key: String = _key

  override def valueConverter: String => T = _valueConverter

  override def strConverter: T => String = _strConverter

  override def doc: String = _doc

  override def version: String = _version

  override def typ: String = _type
}

class ConfigEntryWithDefaultString[T](
   _key: String,
   _defaultVal: String,
   _valueConverter: String => T,
   _strConverter: T => String,
   _doc: String,
   _version: String,
   _type: String) extends ConfigEntry[T] {
  override def defaultValStr: String = _defaultVal

  override def defaultVal: Option[T] = Some(valueConverter(_defaultVal))

  override def readFrom(conf: ConfigProvider): T = {
    val value = readString(conf).getOrElse(_defaultVal)
    valueConverter(value)
  }

  override def key: String = _key

  override def valueConverter: String => T = _valueConverter

  override def strConverter: T => String = _strConverter

  override def doc: String = _doc

  override def version: String = _version

  override def typ: String = _type
}

class ConfigEntryFallback[T](
  _key: String,
  _doc: String,
  _version: String,
  fallback: ConfigEntry[T]) extends ConfigEntry[T] {
  override def defaultValStr: String = fallback.defaultValStr

  override def defaultVal: Option[T] = fallback.defaultVal

  override def readFrom(conf: ConfigProvider): T = {
    readString(conf).map(valueConverter).getOrElse(fallback.readFrom(conf))
  }

  override def key: String = _key

  override def valueConverter: String => T = fallback.valueConverter

  override def strConverter: T => String = fallback.strConverter

  override def doc: String = _doc

  override def version: String = _version

  override def typ: String = fallback.typ
}

object ConfigEntry {
  val UNDEFINED = "<undefined>"

  private val knownConfigs = new java.util.concurrent.ConcurrentHashMap[String, ConfigEntry[_]]()

  def registerEntry(entry: ConfigEntry[_]): Unit = {
    val existing = knownConfigs.putIfAbsent(entry.key, entry)
    require(existing == null, s"Config entry ${entry.key} already registered!")
  }
}
