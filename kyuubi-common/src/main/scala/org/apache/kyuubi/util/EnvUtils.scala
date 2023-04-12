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

import java.util.{Collections, Map => JavaMap}

import scala.collection.JavaConverters._

object EnvUtils {

  /**
   * Portable method for setting env vars on both *nix and Windows. Used for TESTS ONLY.
   * @see http://stackoverflow.com/a/7201825/293064
   * @see https://gist.github.com/vpatryshev/b1bbd15e2b759c157b58b68c58891ff4
   */
  def setEnv(newEnv: Map[String, String]): Unit = {
    try {
      val processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment")
      val theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment")
      theEnvironmentField.setAccessible(true)
      val env = theEnvironmentField.get(null).asInstanceOf[JavaMap[String, String]]
      env.putAll(newEnv.asJava)
      val theCaseInsensitiveEnvironmentField =
        processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment")
      theCaseInsensitiveEnvironmentField.setAccessible(true)
      val cienv = theCaseInsensitiveEnvironmentField.get(null).asInstanceOf[JavaMap[String, String]]
      cienv.putAll(newEnv.asJava)
    } catch {
      case _: NoSuchFieldException =>
        val classes = classOf[Collections].getDeclaredClasses
        val env = System.getenv()
        for { cl <- classes } {
          if (cl.getName == "java.util.Collections$UnmodifiableMap") {
            val field = cl.getDeclaredField("m")
            field.setAccessible(true)
            val obj = field.get(env)
            val map = obj.asInstanceOf[JavaMap[String, String]]
            map.clear()
            map.putAll(newEnv.asJava)
          }
        }
      case e: Throwable => throw e
    }
  }

  def withTestEnv(newEnv: Map[String, String], f: () => Unit): Unit = {
    val originalEnv = System.getenv()
    setEnv(newEnv)
    f.apply()
    setEnv(originalEnv.asScala.toMap)
  }
}
