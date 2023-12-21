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

package org.apache.kyuubi.engine.spark.util

import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object JsonUtils {
  val mapper: ObjectMapper = new ObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .registerModule(DefaultScalaModule)

  def toJson[T](obj: T): String = {
    mapper.writeValueAsString(obj)
  }

  def toPrettyJson[T](obj: T): String = {
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj)
  }

  def fromJson[T](json: String, clz: Class[T]): T = {
    mapper.readValue(json, clz)
  }

  def fromJson[T](json: String)(implicit m: Manifest[T]): T = {
    mapper.readValue(json, m.runtimeClass).asInstanceOf[T]
  }

  def readTree(content: String): JsonNode = {
    mapper.readTree(content)
  }
}
