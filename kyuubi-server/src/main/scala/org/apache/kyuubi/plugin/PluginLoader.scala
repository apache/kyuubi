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

package org.apache.kyuubi.plugin

import scala.util.control.NonFatal

import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.util.reflect.DynConstructors

private[kyuubi] object PluginLoader {

  def loadSessionConfAdvisor(conf: KyuubiConf): Seq[SessionConfAdvisor] = {
    val advisorClass = conf.get(KyuubiConf.SESSION_CONF_ADVISOR)
    if (advisorClass.isEmpty) {
      return new DefaultSessionConfAdvisor() :: Nil
    }
    advisorClass.get.map { advisorClassName =>
      try {
        DynConstructors.builder.impl(advisorClassName)
          .buildChecked[SessionConfAdvisor].newInstance()
      } catch {
        case _: ClassCastException =>
          throw new KyuubiException(
            s"Class $advisorClassName is not a child of '${classOf[SessionConfAdvisor].getName}'.")
        case NonFatal(e) =>
          throw new IllegalArgumentException(s"Error while instantiating '$advisorClassName': ", e)
      }
    }
  }

  def loadGroupProvider(conf: KyuubiConf): GroupProvider = {
    val groupProviderClass = conf.get(KyuubiConf.GROUP_PROVIDER)
    try {
      DynConstructors.builder().impl(groupProviderClass).buildChecked[GroupProvider]().newInstance()
    } catch {
      case _: ClassCastException =>
        throw new KyuubiException(
          s"Class $groupProviderClass is not a child of '${classOf[GroupProvider].getName}'.")
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Error while instantiating '$groupProviderClass': ", e)
    }
  }
}
