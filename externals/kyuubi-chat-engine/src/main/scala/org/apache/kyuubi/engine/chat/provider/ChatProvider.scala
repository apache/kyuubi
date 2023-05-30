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

package org.apache.kyuubi.engine.chat.provider

import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.util.reflect.DynConstructors

trait ChatProvider {

  def open(sessionId: String, user: Option[String] = None): Unit

  def ask(sessionId: String, q: String): String

  def close(sessionId: String): Unit
}

object ChatProvider extends Logging {

  val mapper: ObjectMapper with ClassTagExtensions =
    new ObjectMapper().registerModule(DefaultScalaModule) :: ClassTagExtensions

  def load(conf: KyuubiConf): ChatProvider = {
    val groupProviderClass = conf.get(KyuubiConf.ENGINE_CHAT_PROVIDER)
    try {
      DynConstructors.builder(classOf[ChatProvider])
        .impl(groupProviderClass, classOf[KyuubiConf])
        .impl(groupProviderClass)
        .buildChecked
        .newInstanceChecked(conf)
    } catch {
      case _: ClassCastException =>
        throw new KyuubiException(
          s"Class $groupProviderClass is not a child of '${classOf[ChatProvider].getName}'.")
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Error while instantiating '$groupProviderClass': ", e)
    }
  }
}
