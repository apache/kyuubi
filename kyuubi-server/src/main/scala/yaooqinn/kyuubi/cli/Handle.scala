/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.cli

import java.util.Objects

import org.apache.hive.service.cli.thrift.THandleIdentifier

abstract class Handle(val handleId: HandleIdentifier) {
  def this() = this(new HandleIdentifier())
  def this(tHandleIdentifier: THandleIdentifier) = this(new HandleIdentifier(tHandleIdentifier))

  def getHandleIdentifier: HandleIdentifier = handleId

  override def hashCode: Int = 31 * 1 + Objects.hashCode(handleId)

  override def equals(obj: Any): Boolean = {
    obj match {
      case o: Handle => Objects.equals(handleId, o.handleId)
      case _ => false
    }
  }
}
