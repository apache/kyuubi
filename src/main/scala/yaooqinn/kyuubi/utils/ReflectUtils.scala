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

package yaooqinn.kyuubi.utils

import scala.util.{Failure, Success, Try}

import yaooqinn.kyuubi.Logging

object ReflectUtils extends Logging {
  /**
   * Init a class via Reflection
   * @param className class name
   * @param argTypes arguments class type
   * @param params arguments objects
   * @return instance of className
   */
  def instantiateClass(
      className: String,
      argTypes: Seq[Class[_]],
      params: Seq[AnyRef],
      classLoader: ClassLoader = Thread.currentThread().getContextClassLoader): Any = {
    require(className != null, "class name could not be null!")
    try {
      if (argTypes!= null && argTypes.nonEmpty) {
        require(argTypes.length == params.length, "each params should have a class type!")
        classLoader.loadClass(className).getConstructor(argTypes: _*)
          .newInstance(params: _*)
      } else {
        classLoader.loadClass(className).getConstructor().newInstance()
      }
    } catch {
      case e: Exception => throw e
    }
  }

  /**
   * Init a class via Reflection
   * @param className class name
   * @return
   */
  def instantiateClassByName(
      className: String,
      classLoader: ClassLoader = Thread.currentThread().getContextClassLoader): Any = {
    instantiateClass(className, Seq.empty, Seq.empty, classLoader)
  }

  /**
   * Find the class of the give class name
   * @param className class name
   * @param silence set false to log err while class not being found
   * @return optional class object
   */
  def findClass(className: String, silence: Boolean): Class[_] = {
    try {
      Class.forName(className)
    } catch {
      case e: Exception =>
        if (!silence) {
          throw e
        } else {
          error(e.getMessage, e)
          null
        }
    }
  }

  /**
   * Find the class of the give class name
   * @param className class name
   * @return optional class object
   */
  def findClass(className: String): Class[_] = {
    findClass(className = className, silence = false)
  }

  /**
   * Invoke static method via reflection
   * @param c class object
   * @param name method name
   * @return
   */
  def invokeStaticMethod(c: Class[_], name: String): Any = {
    invokeStaticMethod(c, name, Seq.empty, Seq.empty)
  }

  /**
   * Invoke static method via reflection
   * @param c class object
   * @param name method name
   * @param argTypes argument class types
   * @param params argument objects
   * @return
   */
  def invokeStaticMethod(
      c: Class[_],
      name: String,
      argTypes: Seq[Class[_]], params: Seq[AnyRef]): Any = {
    Try { c.getMethod(name, argTypes: _*).invoke(null, params: _*) } match {
      case Success(value) => value
      case Failure(exception) => throw exception
    }
  }

  /**
   * Invoke a method of an object via reflection
   * @param o object
   * @param name method name
   * @return
   */
  def invokeMethod(o: Any, name: String): Any = {
    invokeMethod(o, name, Seq.empty, Seq.empty)
  }

  /**
   * Invoke a method of an object via reflection
   * @param o object
   * @param name method name
   * @param argTypes arguments class type
   * @param params arguments object list
   * @return
   */
  def invokeMethod(
      o: Any,
      name: String,
      argTypes: Seq[Class[_]], params: Seq[AnyRef]): Any = {
    require(o != null, "object could not be null!")
    Try {o.getClass.getMethod(name, argTypes: _*).invoke(o, params: _*) } match {
      case Success(value) => value
      case Failure(exception) => throw exception
    }
  }
}
