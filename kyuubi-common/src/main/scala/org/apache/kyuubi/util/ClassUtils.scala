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

package org.apache.kyuubi.util

import org.apache.kyuubi.config.KyuubiConf


object ClassUtils {
  /**
   * Create an object instance with given [[KyuubiConf]].
   * @param clazz object class
   * @param conf configuration ([[KyuubiConf]])
   * @tparam T object instance type to create
   * @return
   */
  def createInstance[T](clazz: Class[_], conf: KyuubiConf): T = {
    val confConstructor = clazz.getConstructors.exists(p => {
      val params = p.getParameterTypes
      params.length == 1 && classOf[KyuubiConf].isAssignableFrom(params(0))
    })
    if (confConstructor) {
      clazz.getConstructor(classOf[KyuubiConf]).newInstance(conf)
        .asInstanceOf[T]
    } else {
      clazz.newInstance().asInstanceOf[T]
    }
  }
}
