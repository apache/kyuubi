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

package org.apache.kyuubi.plugin.tag

import org.apache.kyuubi.plugin.tag.TagLevel.TagLevel

trait Tag {
  def getKey(): String
}

abstract class BaseTag(level: TagLevel) extends Tag {
  override def getKey(): String = s"__${level.toString}__"
}

abstract class NamedTag(level: TagLevel, name: String) extends Tag {
  override def getKey(): String = s"__${level.toString}_${name}__"
}

case class SystemTag() extends BaseTag(TagLevel.SYSTEM)
case class ServerTag(server: String) extends NamedTag(TagLevel.SERVER, server)
case class UserTag(user: String) extends NamedTag(TagLevel.USER, user)
case class OriginTag(tag: String) extends NamedTag(TagLevel.TAG, tag)

object TagLevel extends Enumeration {
  type TagLevel = Value
  val SYSTEM, SERVER, USER, TAG = Value
}
