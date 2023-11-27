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
import java.util.UUID

import org.apache.kyuubi.shaded.hive.service.rpc.thrift.THandleIdentifier

private[kyuubi] object Handle {
  final private val SECRET_ID = UUID.fromString("c2ee5b97-3ea0-41fc-ac16-9bd708ed8f38")

  def toTHandleIdentifier(publicId: UUID): THandleIdentifier = {
    val guid = new Array[Byte](16)
    val pbb = ByteBuffer.wrap(guid)
    val secret = new Array[Byte](16)
    val sbb = ByteBuffer.wrap(secret)
    pbb.putLong(publicId.getMostSignificantBits)
    pbb.putLong(publicId.getLeastSignificantBits)
    sbb.putLong(SECRET_ID.getMostSignificantBits)
    sbb.putLong(SECRET_ID.getLeastSignificantBits)
    new THandleIdentifier(ByteBuffer.wrap(guid), ByteBuffer.wrap(secret))
  }

  def fromTHandleIdentifier(tHandleId: THandleIdentifier): UUID = {
    val pbb = ByteBuffer.wrap(tHandleId.getGuid)
    new UUID(pbb.getLong, pbb.getLong)
  }
}
