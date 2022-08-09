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

package org.apache.kyuubi.restore.kvstore

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

class KVStoreSerializer {

  protected var mapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def serialize(o: Any): Array[Byte] =
    if (o.isInstanceOf[String]) {
      o.asInstanceOf[String].getBytes(UTF_8)
    } else {
      val bytes = new ByteArrayOutputStream
      var out: GZIPOutputStream = null
      try {
        out = new GZIPOutputStream(bytes)
        mapper.writeValue(out, o)
      } finally {
        if (out != null) out.close()
      }
      bytes.toByteArray
    }

  def deserialize[T](data: Array[Byte], klass: Class[T]): T = {
    if (klass == classOf[String]) {
      new String(data, UTF_8).asInstanceOf[T]
    } else {
      var in: GZIPInputStream = null
      try {
        in = new GZIPInputStream(new ByteArrayInputStream(data))
        mapper.readValue(in, klass)
      } finally {
        if (in != null) in.close()
      }
    }
  }

}
