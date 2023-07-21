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

package org.apache.kyuubi.server.trino.api

import javax.ws.rs.ext.ContextResolver

import com.fasterxml.jackson.databind.{DeserializationFeature, MapperFeature, ObjectMapper}
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module

class TrinoScalaObjectMapper extends ContextResolver[ObjectMapper] {

  // refer `io.trino.client.JsonCodec`
  private lazy val mapper = new ObjectMapper()
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    .disable(MapperFeature.AUTO_DETECT_CREATORS)
    .disable(MapperFeature.AUTO_DETECT_FIELDS)
    .disable(MapperFeature.AUTO_DETECT_SETTERS)
    .disable(MapperFeature.AUTO_DETECT_GETTERS)
    .disable(MapperFeature.AUTO_DETECT_IS_GETTERS)
    .disable(MapperFeature.USE_GETTERS_AS_SETTERS)
    .disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
    .disable(MapperFeature.INFER_PROPERTY_MUTATORS)
    .disable(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS)
    .registerModule(new Jdk8Module)

  override def getContext(aClass: Class[_]): ObjectMapper = mapper
}
