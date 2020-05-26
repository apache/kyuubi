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

  def defaultValStr: String
  def defaultVal: Option[T] = None

  final override def toString: String = {
    s"ConfigEntry(key=$key, defaultValue=$defaultValStr, doc=$doc, version=$version)"
  }

  final protected def readString(provider: ConfigProvider): Option[String] = {
    provider.get(key)
  }

  def readFrom(conf: ConfigProvider): T

  ConfigEntry.registerEntry(this)
}

case class OptionalConfigEntry[T](
    key: String,
    rawValueConverter: String => T,
    rawStrConverter: T => String,
    doc: String,
    version: String) extends ConfigEntry[Option[T]] {
  override def valueConverter: String => Option[T] = s => Option(rawValueConverter(s))

  override def strConverter: Option[T] => String = v => v.map(rawStrConverter).orNull

  override def defaultValStr: String = ConfigEntry.UNDEFINED

  override def readFrom(conf: ConfigProvider): Option[T] = {
    readString(conf).map(rawValueConverter)
  }
}

case class ConfigEntryWithDefault[T](
    key: String,
    _defaultVal: T,
    valueConverter: String => T,
    strConverter: T => String,
    doc: String,
    version: String) extends ConfigEntry[T] {
  override def defaultValStr: String = strConverter(_defaultVal)

  override def defaultVal: Option[T] = Option(_defaultVal)

  override def readFrom(conf: ConfigProvider): T = {
    readString(conf).map(valueConverter).getOrElse(_defaultVal)
  }
}

case class ConfigEntryWithDefaultString[T](
   key: String,
   _defaultVal: String,
   valueConverter: String => T,
   strConverter: T => String,
   doc: String,
   version: String) extends ConfigEntry[T] {
  override def defaultValStr: String = _defaultVal

  override def defaultVal: Option[T] = Some(valueConverter(_defaultVal))

  override def readFrom(conf: ConfigProvider): T = {
    val value = readString(conf).getOrElse(_defaultVal)
    valueConverter(value)
  }
}

object ConfigEntry {
  val UNDEFINED = "<undefined>"

  private val knownConfigs = new java.util.concurrent.ConcurrentHashMap[String, ConfigEntry[_]]()

  def registerEntry(entry: ConfigEntry[_]): Unit = {
    val existing = knownConfigs.putIfAbsent(entry.key, entry)
    require(existing == null, s"Config entry ${entry.key} already registered!")
  }

  def findEntry(key: String): ConfigEntry[_] = knownConfigs.get(key)
}
