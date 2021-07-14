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

package org.apache.kyuubi.ha.v2.engine

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.kyuubi.ha.v2.InstanceSerializer
import org.apache.kyuubi.ha.v2.engine.EngineInstanceSerializer._

class EngineInstanceSerializer extends InstanceSerializer[EngineInstance] {

  /**
   * Serialize an instance into bytes
   *
   * @param instance the instance
   * @return byte array representing the instance
   * @throws Exception any errors
   */
  override def serialize(instance: EngineInstance): Array[Byte] = {
    MAPPER.writeValueAsBytes(instance)
  }

  /**
   * Deserialize a byte array into an instance
   *
   * @param bytes the bytes
   * @return service instance
   * @throws Exception any errors
   */
  override def deserialize(bytes: Array[Byte]): EngineInstance = {
    MAPPER.readValue(bytes, classOf[EngineInstance])
  }

}

object EngineInstanceSerializer {

  private val MAPPER: ObjectMapper = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper
  }

  private val SERIALIZER: EngineInstanceSerializer = new EngineInstanceSerializer

  def serialize(instance: EngineInstance): Array[Byte] = SERIALIZER.serialize(instance)

  def deserialize(bytes: Array[Byte]): EngineInstance = SERIALIZER.deserialize(bytes)

}
