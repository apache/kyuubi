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

package org.apache.spark.ui

import java.lang.reflect.{InvocationHandler, Method}
import java.util

class JavaxHttpServletRequestProxy(req: javax.servlet.http.HttpServletRequest)
  extends InvocationHandler {

  override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef =
    method.invoke(req, args)
}

class JakartaHttpServletRequestProxy(req: jakarta.servlet.http.HttpServletRequest)
  extends InvocationHandler {

  override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef =
    method.invoke(req, args)
}

trait HttpServletRequestLike {

  def getParameter(name: String): String

  def getParameterMap: util.Map[String, Array[String]]
}

object HttpServletRequestLike {
  def fromJavax(req: javax.servlet.http.HttpServletRequest)
      : HttpServletRequestLike with javax.servlet.http.HttpServletRequest = {
    java.lang.reflect.Proxy.newProxyInstance(
      org.apache.spark.util.Utils.getContextOrSparkClassLoader,
      Array(classOf[javax.servlet.http.HttpServletRequest], classOf[HttpServletRequestLike]),
      new JavaxHttpServletRequestProxy(req)).asInstanceOf
  }

  def fromJakarta(req: jakarta.servlet.http.HttpServletRequest)
      : HttpServletRequestLike with jakarta.servlet.http.HttpServletRequest = {
    java.lang.reflect.Proxy.newProxyInstance(
      org.apache.spark.util.Utils.getContextOrSparkClassLoader,
      Array(classOf[jakarta.servlet.http.HttpServletRequest], classOf[HttpServletRequestLike]),
      new JakartaHttpServletRequestProxy(req)).asInstanceOf
  }
}
