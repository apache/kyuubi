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
package org.apache.spark.kyuubi

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.SparkException

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.engine.spark.events.SparkOperationEvent

class KyuubiSparkEventSuite extends KyuubiFunSuite {

  test("test exception serializer and deserializer of SparkOperationEvent") {
    val exception = new SparkException("message", new Exception("cause"))
    val event = new SparkOperationEvent(
      "statementId",
      "statement",
      shouldRunAsync = true,
      "state",
      0L,
      0L,
      0L,
      0L,
      Some(exception),
      "sessionId",
      "sessionUser",
      None,
      None,
      None)
    val mapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val json = mapper.writeValueAsString(event)
    assert(json.contains("\"exception\":{\"Message\":\"message\",\"Stack Trace\":" +
      "[{\"Declaring Class\":\"org.apache.spark.kyuubi.KyuubiSparkEventSuite\","))
    val deserializeEvent = mapper.readValue(json, classOf[SparkOperationEvent])
    assert(deserializeEvent.exception.isDefined)
    assert(deserializeEvent.exception.get.getMessage === "message")
    assert(deserializeEvent.exception.get.getStackTrace.length > 0)
  }

}
