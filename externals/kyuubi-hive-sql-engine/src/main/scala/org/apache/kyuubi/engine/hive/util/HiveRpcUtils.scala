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

package org.apache.kyuubi.engine.hive.util

import org.apache.hive.service.rpc.thrift.{TFetchOrientation => HiveTFetchOrientation, THandleIdentifier => HiveTHandleIdentifier, TProtocolVersion => HiveTProtocolVersion, TRowSet => HiveTRowSet, TSessionHandle => HiveTSessionHandle, TTableSchema => HiveTTableSchema}
import org.apache.thrift.protocol.{TCompactProtocol => HiveTCompactProtocol}
import org.apache.thrift.transport.{TMemoryBuffer => HiveTMemoryBuffer}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._
import org.apache.kyuubi.shaded.thrift.protocol.TCompactProtocol
import org.apache.kyuubi.shaded.thrift.transport.TMemoryInputTransport

object HiveRpcUtils extends Logging {

  def asHive(tProtocolVersion: TProtocolVersion): HiveTProtocolVersion =
    Option(HiveTProtocolVersion.findByValue(tProtocolVersion.getValue)).getOrElse {
      val latestHiveTProtocolVersion = HiveTProtocolVersion.values().last
      warn(s"Unsupported TProtocolVersion (Kyuubi): $tProtocolVersion, " +
        s"fallback to latest TProtocolVersion (Hive): $latestHiveTProtocolVersion")
      latestHiveTProtocolVersion
    }

  def asHive(tHandleIdentifier: THandleIdentifier): HiveTHandleIdentifier =
    new HiveTHandleIdentifier(
      tHandleIdentifier.bufferForGuid(),
      tHandleIdentifier.bufferForSecret())

  def asHive(tSessionHandle: TSessionHandle): HiveTSessionHandle =
    new HiveTSessionHandle(asHive(tSessionHandle.getSessionId))

  def asHive(tFetchOrientation: TFetchOrientation): HiveTFetchOrientation =
    Option(HiveTFetchOrientation.findByValue(tFetchOrientation.getValue)).getOrElse {
      throw new UnsupportedOperationException(
        s"Unsupported TFetchOrientation (Kyuubi): $tFetchOrientation")
    }

  def asKyuubi(hiveTTableSchema: HiveTTableSchema): TTableSchema = {
    val hiveBuffer = new HiveTMemoryBuffer(128)
    hiveTTableSchema.write(new HiveTCompactProtocol(hiveBuffer))
    val bytes = hiveBuffer.getArray
    val kyuubiBuffer = new TMemoryInputTransport(bytes)
    val kyuubiTTableSchema = new TTableSchema
    kyuubiTTableSchema.read(new TCompactProtocol(kyuubiBuffer))
    kyuubiTTableSchema
  }

  def asKyuubi(hiveTRowSet: HiveTRowSet): TRowSet = {
    val hiveBuffer = new HiveTMemoryBuffer(128)
    hiveTRowSet.write(new HiveTCompactProtocol(hiveBuffer))
    val bytes = hiveBuffer.getArray
    val kyuubiBuffer = new TMemoryInputTransport(bytes)
    val kyuubiTRowSet = new TRowSet
    kyuubiTRowSet.read(new TCompactProtocol(kyuubiBuffer))
    kyuubiTRowSet
  }
}
