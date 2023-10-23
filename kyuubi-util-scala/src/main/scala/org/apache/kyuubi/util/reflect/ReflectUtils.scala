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

package org.apache.kyuubi.util.reflect

import java.util.ServiceLoader

import scala.collection.JavaConverters._
import scala.language.existentials
import scala.reflect.ClassTag
import scala.util.Try

object ReflectUtils {

  /**
   * Determines whether the provided class is loadable
   * @param className the class name
   * @param cl the class loader
   * @return is the class name loadable with the class loader
   */
  def isClassLoadable(
      className: String,
      cl: ClassLoader = Thread.currentThread().getContextClassLoader): Boolean =
    Try {
      DynClasses.builder().loader(cl).impl(className).buildChecked()
    }.isSuccess

  /**
   * get the field value of the given object
   * @param target the target object
   * @param fieldName the field name from declared field names
   * @tparam T the expected return class type
   * @return
   */
  def getField[T](target: AnyRef, fieldName: String): T = {
    val (clz, obj) = getTargetClass(target)
    try {
      val field = DynFields.builder
        .hiddenImpl(clz, fieldName)
        .impl(clz, fieldName)
        .build[T]
      if (field.isStatic) {
        field.asStatic.get
      } else {
        field.bind(obj).get
      }
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"$clz does not have $fieldName field", e)
    }
  }

  /**
   * Invoke a method with the given name and arguments on the given target object.
   * @param target the target object
   * @param methodName the method name from declared field names
   * @param args pairs of class and values for the arguments
   * @tparam T the expected return class type,
   *           returning type Nothing if it's not provided or inferable
   * @return
   */
  def invokeAs[T](target: AnyRef, methodName: String, args: (Class[_], AnyRef)*): T = {
    val (clz, obj) = getTargetClass(target)
    val argClasses = args.map(_._1)
    try {
      val method = DynMethods.builder(methodName)
        .hiddenImpl(clz, argClasses: _*)
        .impl(clz, argClasses: _*)
        .buildChecked
      if (method.isStatic) {
        method.asStatic.invoke[T](args.map(_._2): _*)
      } else {
        method.bind(obj).invoke[T](args.map(_._2): _*)
      }
    } catch {
      case e: Exception =>
        throw new RuntimeException(
          s"$clz does not have $methodName${argClasses.map(_.getName).mkString("(", ", ", ")")}",
          e)
    }
  }

  /**
   * Creates a iterator for with a new service loader for the given service type and class
   * loader.
   *
   * @param cl The class loader to be used to load provider-configuration files
   *           and provider classes
   * @param ct class tag of the generics class type
   * @tparam T the class of the service type
   * @return
   */
  def loadFromServiceLoader[T](cl: ClassLoader = Thread.currentThread().getContextClassLoader)(
      implicit ct: ClassTag[T]): Iterator[T] =
    ServiceLoader.load(ct.runtimeClass, cl).iterator().asScala.map(_.asInstanceOf[T])

  private def getTargetClass(target: AnyRef): (Class[_], AnyRef) = target match {
    case clz: Class[_] => (clz, None)
    case clzName: String => (DynClasses.builder.impl(clzName).buildChecked, None)
    case (clz: Class[_], o: AnyRef) => (clz, o)
    case (clzName: String, o: AnyRef) => (DynClasses.builder.impl(clzName).buildChecked, o)
    case o => (o.getClass, o)
  }
}
