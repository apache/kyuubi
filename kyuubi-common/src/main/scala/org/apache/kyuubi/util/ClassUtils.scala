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

import scala.util.Try

import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.config.KyuubiConf

object ClassUtils {

  /**
   * Create an object instance with given class name and [[KyuubiConf]].
   * @param className the class name
   * @param expected the expected class type
   * @param conf configuration ([[KyuubiConf]])
   * @tparam T object instance type to create
   * @return
   */
  def createInstance[T](className: String, expected: Class[T], conf: KyuubiConf): T = {
    val classLoader = Thread.currentThread.getContextClassLoader
    val cls = Class.forName(className, true, classLoader)
    cls match {
      case clazz if expected.isAssignableFrom(cls) =>
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
      case _ => throw new KyuubiException(
          s"$className must extend of ${expected.getName}")
    }
  }

  /** Determines whether the provided class is loadable. */
  def classIsLoadable(
      clazz: String,
      cl: ClassLoader = Thread.currentThread().getContextClassLoader): Boolean = {
    Try { Class.forName(clazz, false, cl) }.isSuccess
  }
}
