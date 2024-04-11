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

package org.apache.kyuubi.events

import scala.collection.JavaConverters._

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonNode, JsonSerializer, ObjectMapper, SerializerProvider}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object JsonProtocol {

  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def productToJson[T <: KyuubiEvent](value: T): String = mapper.writeValueAsString(value)

  def jsonToEvent[T <: KyuubiEvent](jsonValue: String, cls: Class[T]): KyuubiEvent = {
    mapper.readValue(jsonValue, cls)
  }
}

// Exception serializer and deserializer, copy from org.apache.spark.util.JsonProtocol
class ExceptionSerializer extends JsonSerializer[Exception] {

  override def serialize(
      value: Exception,
      gen: JsonGenerator,
      serializers: SerializerProvider): Unit = {
    exceptionToJson(value, gen)
  }

  private def exceptionToJson(exception: Exception, g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeStringField("Message", exception.getMessage)
    g.writeFieldName("Stack Trace")
    stackTraceToJson(exception.getStackTrace, g)
    g.writeEndObject()
  }

  private def stackTraceToJson(stackTrace: Array[StackTraceElement], g: JsonGenerator): Unit = {
    g.writeStartArray()
    stackTrace.foreach { line =>
      g.writeStartObject()
      g.writeStringField("Declaring Class", line.getClassName)
      g.writeStringField("Method Name", line.getMethodName)
      g.writeStringField("File Name", line.getFileName)
      g.writeNumberField("Line Number", line.getLineNumber)
      g.writeEndObject()
    }
    g.writeEndArray()
  }
}

class ExceptionDeserializer extends JsonDeserializer[Exception] {

  override def deserialize(jsonParser: JsonParser, ctxt: DeserializationContext): Exception = {
    val jsonNode = jsonParser.readValueAsTree[JsonNode]()
    exceptionFromJson(jsonNode)
  }

  private def exceptionFromJson(json: JsonNode): Exception = {
    val message = jsonOption(json.get("Message")).map(_.extractString).orNull
    val e = new Exception(message)
    e.setStackTrace(stackTraceFromJson(json.get("Stack Trace")))
    e
  }

  private def stackTraceFromJson(json: JsonNode): Array[StackTraceElement] = {
    jsonOption(json).map(_.extractElements.map { line =>
      val declaringClass = line.get("Declaring Class").extractString
      val methodName = line.get("Method Name").extractString
      val fileName = jsonOption(line.get("File Name")).map(_.extractString).orNull
      val lineNumber = line.get("Line Number").extractInt
      new StackTraceElement(declaringClass, methodName, fileName, lineNumber)
    }.toArray).getOrElse(Array[StackTraceElement]())
  }

  private def jsonOption(json: JsonNode): Option[JsonNode] = {
    if (json == null || json.isNull) {
      None
    } else {
      Some(json)
    }
  }

  implicit private class JsonNodeImplicits(json: JsonNode) {
    def extractElements: Iterator[JsonNode] = {
      require(json.isContainerNode, s"Expected container, got ${json.getNodeType}")
      json.elements.asScala
    }

    def extractInt: Int = {
      require(json.isNumber, s"Expected number, got ${json.getNodeType}")
      json.intValue
    }

    def extractString: String = {
      require(json.isTextual || json.isNull, s"Expected string or NULL, got ${json.getNodeType}")
      json.textValue
    }
  }
}
