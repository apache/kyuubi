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

package org.apache.kyuubi.cli

import java.nio.ByteBuffer
import java.util.{Objects, UUID}

import org.apache.hive.service.rpc.thrift.THandleIdentifier

class HandleIdentifier(val publicId: UUID, val secretId: UUID) {

  def toTHandleIdentifier: THandleIdentifier = {
    val guid = new Array[Byte](16)
    val pbb = ByteBuffer.wrap(guid)
    val secret = new Array[Byte](16)
    val sbb = ByteBuffer.wrap(secret)
    pbb.putLong(publicId.getMostSignificantBits)
    pbb.putLong(publicId.getLeastSignificantBits)
    sbb.putLong(secretId.getMostSignificantBits)
    sbb.putLong(secretId.getLeastSignificantBits)
    new THandleIdentifier(ByteBuffer.wrap(guid), ByteBuffer.wrap(secret))
  }

  override def hashCode(): Int = (Objects.hashCode(publicId) + 31) * 31 + Objects.hashCode(secretId)

  override def equals(obj: Any): Boolean = obj match {
    case i: HandleIdentifier =>
      Objects.equals(publicId, i.publicId) && Objects.equals(secretId, i.secretId)
    case _ => false
  }

  override def toString: String = publicId.toString
}

object HandleIdentifier {

  def apply(publicId: UUID, secretId: UUID): HandleIdentifier = {
    new HandleIdentifier(publicId, secretId)
  }

  def apply(): HandleIdentifier = {
    val publicId = UUID.randomUUID()
    val secretId = UUID.randomUUID()
    new HandleIdentifier(publicId, secretId)
  }

  def apply(tHandleId: THandleIdentifier): HandleIdentifier = {
    val pbb = ByteBuffer.wrap(tHandleId.getGuid)
    val publicId = new UUID(pbb.getLong, pbb.getLong)

    val sbb = ByteBuffer.wrap(tHandleId.getSecret)
    val secretId = new UUID(sbb.getLong, sbb.getLong)

    new HandleIdentifier(publicId, secretId)
  }
}
