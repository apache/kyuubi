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

package org.apache.kyuubi

import java.net.URL

import scala.collection.immutable.SortedMap

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * Information associated with an error class.
 */
private[kyuubi] case class ErrorInfo(
    message: String,
    sqlState: Option[String]) {}

object KyuubiThrowableHelper {
  val errorClassesUrl: URL =
    Utils.getKyuubiClassLoader.getResource("error/error-classes.json")
  val errorClassToInfoMap: SortedMap[String, ErrorInfo] = {
    val mapper: JsonMapper = JsonMapper.builder()
      .addModule(DefaultScalaModule)
      .build()
    mapper.readValue(errorClassesUrl, new TypeReference[SortedMap[String, ErrorInfo]]() {})
  }

  def getMessage(errorClass: String): String = {
    val errorInfo = errorClassToInfoMap.getOrElse(
      errorClass,
      throw new IllegalArgumentException(s"Cannot find error class '$errorClass'"))

    errorInfo.message
  }

  def getParameterMessage(errorClass: String, parameters: String*): String = {
    String.format(getMessage(errorClass), parameters: _*)
  }

  def getSqlState(errorClass: String): String = {
    Option(errorClass).flatMap(errorClassToInfoMap.get).flatMap(_.sqlState).orNull
  }

  def getErrorInfo(errorClass: String): (String, String) = {
    (getMessage(errorClass), getSqlState(errorClass))
  }

  def getParameterErrorInfo(errorClass: String, parameters: String*): (String, String) = {
    (getParameterMessage(errorClass, parameters: _*), getSqlState(errorClass))
  }
}
