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

package org.apache.kyuubi.service.authentication

import com.fasterxml.jackson.annotation.{JsonAutoDetect, JsonInclude, JsonPropertyOrder}
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

@JsonInclude(Include.NON_ABSENT)
@JsonAutoDetect(getterVisibility = Visibility.ANY, setterVisibility = Visibility.ANY)
@JsonPropertyOrder(alphabetic = true)
class KyuubiInternalAccessIdentifier {
  var issueDate: Long = Integer.MAX_VALUE
  var maxDate: Long = 0

  def toJson: String = {
    KyuubiInternalAccessIdentifier.mapper.writeValueAsString(this)
  }
}

private[kyuubi] object KyuubiInternalAccessIdentifier {
  private val mapper = new ObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .enable(SerializationFeature.INDENT_OUTPUT)
    .registerModule(DefaultScalaModule)

  def fromJson(json: String): KyuubiInternalAccessIdentifier = {
    mapper.readValue(json, classOf[KyuubiInternalAccessIdentifier])
  }

  def newIdentifier(maxDate: Long): KyuubiInternalAccessIdentifier = {
    val identifier = new KyuubiInternalAccessIdentifier
    identifier.issueDate = System.currentTimeMillis()
    identifier.maxDate = maxDate
    identifier
  }
}
