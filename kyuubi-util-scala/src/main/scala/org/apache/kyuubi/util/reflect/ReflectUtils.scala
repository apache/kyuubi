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
  def getField[T](target: Any, fieldName: String): T = {
    val targetClass = target.getClass
    try {
      DynFields.builder()
        .hiddenImpl(targetClass, fieldName)
        .buildChecked[T](target)
        .get()
    } catch {
      case e: Exception =>
        val candidates = targetClass.getDeclaredFields.map(_.getName).sorted
        throw new RuntimeException(
          s"Field $fieldName not in $targetClass [${candidates.mkString(",")}]",
          e)
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
    val targetClass = target.getClass
    val argClasses = args.map(_._1)
    try {
      DynMethods.builder(methodName)
        .hiddenImpl(targetClass, argClasses: _*)
        .buildChecked(target)
        .invoke[T](args.map(_._2): _*)
    } catch {
      case e: Exception =>
        val candidates = targetClass.getDeclaredMethods.map(_.getName).sorted
        val argClassesNames = argClasses.map(_.getClass.getName)
        throw new RuntimeException(
          s"Method $methodName (${argClassesNames.mkString(",")})" +
            s" not found in $targetClass [${candidates.mkString(",")}]",
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
}
