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

import java.io.BufferedReader
import java.security.Principal
import java.util
import java.util.Locale

class JavaxHttpServletRequest(req: javax.servlet.http.HttpServletRequest)
  extends javax.servlet.http.HttpServletRequest with HttpServletRequestLike {
  override def getAuthType: String = req.getAuthType

  override def getCookies: Array[javax.servlet.http.Cookie] = req.getCookies

  override def getDateHeader(s: String): Long = req.getDateHeader(s)

  override def getHeader(s: String): String = req.getHeader(s)

  override def getHeaders(s: String): util.Enumeration[String] = req.getHeaders(s)

  override def getHeaderNames: util.Enumeration[String] = req.getHeaderNames

  override def getIntHeader(s: String): Int = req.getIntHeader(s)

  override def getMethod: String = req.getMethod

  override def getPathInfo: String = req.getPathInfo

  override def getPathTranslated: String = req.getPathTranslated

  override def getContextPath: String = req.getContextPath

  override def getQueryString: String = req.getQueryString

  override def getRemoteUser: String = req.getRemoteUser

  override def isUserInRole(s: String): Boolean = req.isUserInRole(s)

  override def getUserPrincipal: Principal = req.getUserPrincipal

  override def getRequestedSessionId: String = req.getRequestedSessionId

  override def getRequestURI: String = req.getRequestURI

  override def getRequestURL: StringBuffer = req.getRequestURL

  override def getServletPath: String = req.getServletPath

  override def getSession(b: Boolean): javax.servlet.http.HttpSession = req.getSession(b)

  override def getSession: javax.servlet.http.HttpSession = req.getSession

  override def changeSessionId(): String = req.changeSessionId()

  override def isRequestedSessionIdValid: Boolean = req.isRequestedSessionIdValid

  override def isRequestedSessionIdFromCookie: Boolean = req.isRequestedSessionIdFromCookie

  override def isRequestedSessionIdFromURL: Boolean = req.isRequestedSessionIdFromURL

  override def isRequestedSessionIdFromUrl: Boolean = req.isRequestedSessionIdFromUrl

  override def authenticate(httpServletResponse: javax.servlet.http.HttpServletResponse): Boolean =
    req.authenticate(httpServletResponse)

  override def login(s: String, s1: String): Unit = req.login(s, s1)

  override def logout(): Unit = req.logout()

  override def getParts: util.Collection[javax.servlet.http.Part] = req.getParts

  override def getPart(s: String): javax.servlet.http.Part = req.getPart(s)

  override def upgrade[T <: javax.servlet.http.HttpUpgradeHandler](aClass: Class[T]): T =
    req.upgrade(aClass)

  override def getAttribute(s: String): AnyRef = req.getAttribute(s)

  override def getAttributeNames: util.Enumeration[String] = req.getAttributeNames

  override def getCharacterEncoding: String = req.getCharacterEncoding

  override def setCharacterEncoding(s: String): Unit = req.setCharacterEncoding(s)

  override def getContentLength: Int = req.getContentLength

  override def getContentLengthLong: Long = req.getContentLengthLong

  override def getContentType: String = req.getContentType

  override def getInputStream: javax.servlet.ServletInputStream = req.getInputStream

  override def getParameter(s: String): String = req.getParameter(s)

  override def getParameterNames: util.Enumeration[String] = req.getParameterNames

  override def getParameterValues(s: String): Array[String] = req.getParameterValues(s)

  override def getParameterMap: util.Map[String, Array[String]] = req.getParameterMap

  override def getProtocol: String = req.getProtocol

  override def getScheme: String = req.getScheme

  override def getServerName: String = req.getServerName

  override def getServerPort: Int = req.getServerPort

  override def getReader: BufferedReader = req.getReader

  override def getRemoteAddr: String = req.getRemoteAddr

  override def getRemoteHost: String = req.getRemoteHost

  override def setAttribute(s: String, o: Any): Unit = req.setAttribute(s, o)

  override def removeAttribute(s: String): Unit = req.removeAttribute(s)

  override def getLocale: Locale = req.getLocale

  override def getLocales: util.Enumeration[Locale] = req.getLocales

  override def isSecure: Boolean = req.isSecure

  override def getRequestDispatcher(s: String): javax.servlet.RequestDispatcher =
    req.getRequestDispatcher(s)

  override def getRealPath(s: String): String = req.getRealPath(s)

  override def getRemotePort: Int = req.getRemotePort

  override def getLocalName: String = req.getLocalName

  override def getLocalAddr: String = req.getLocalAddr

  override def getLocalPort: Int = req.getLocalPort

  override def getServletContext: javax.servlet.ServletContext = req.getServletContext

  override def startAsync(): javax.servlet.AsyncContext = req.startAsync()

  override def startAsync(
      servletRequest: javax.servlet.ServletRequest,
      servletResponse: javax.servlet.ServletResponse): javax.servlet.AsyncContext =
    req.startAsync(servletRequest, servletResponse)

  override def isAsyncStarted: Boolean = req.isAsyncStarted

  override def isAsyncSupported: Boolean = req.isAsyncSupported

  override def getAsyncContext: javax.servlet.AsyncContext = req.getAsyncContext

  override def getDispatcherType: javax.servlet.DispatcherType = req.getDispatcherType
}

class JakartaHttpServletRequest(req: jakarta.servlet.http.HttpServletRequest)
  extends jakarta.servlet.http.HttpServletRequest with HttpServletRequestLike {
  override def getAuthType: String = req.getAuthType

  override def getCookies: Array[jakarta.servlet.http.Cookie] = req.getCookies

  override def getDateHeader(s: String): Long = req.getDateHeader(s)

  override def getHeader(s: String): String = req.getHeader(s)

  override def getHeaders(s: String): util.Enumeration[String] = req.getHeaders(s)

  override def getHeaderNames: util.Enumeration[String] = req.getHeaderNames

  override def getIntHeader(s: String): Int = req.getIntHeader(s)

  override def getMethod: String = req.getMethod

  override def getPathInfo: String = req.getPathInfo

  override def getPathTranslated: String = req.getPathTranslated

  override def getContextPath: String = req.getContextPath

  override def getQueryString: String = req.getQueryString

  override def getRemoteUser: String = req.getRemoteUser

  override def isUserInRole(s: String): Boolean = req.isUserInRole(s)

  override def getUserPrincipal: Principal = req.getUserPrincipal

  override def getRequestedSessionId: String = req.getRequestedSessionId

  override def getRequestURI: String = req.getRequestURI

  override def getRequestURL: StringBuffer = req.getRequestURL

  override def getServletPath: String = req.getServletPath

  override def getSession(b: Boolean): jakarta.servlet.http.HttpSession = req.getSession(b)

  override def getSession: jakarta.servlet.http.HttpSession = req.getSession

  override def changeSessionId(): String = req.changeSessionId()

  override def isRequestedSessionIdValid: Boolean = req.isRequestedSessionIdValid

  override def isRequestedSessionIdFromCookie: Boolean = req.isRequestedSessionIdFromCookie

  override def isRequestedSessionIdFromURL: Boolean = req.isRequestedSessionIdFromURL

  override def isRequestedSessionIdFromUrl: Boolean = req.isRequestedSessionIdFromUrl

  override def authenticate(httpServletResponse: jakarta.servlet.http.HttpServletResponse)
      : Boolean =
    req.authenticate(httpServletResponse)

  override def login(s: String, s1: String): Unit = req.login(s, s1)

  override def logout(): Unit = req.logout()

  override def getParts: util.Collection[jakarta.servlet.http.Part] = req.getParts

  override def getPart(s: String): jakarta.servlet.http.Part = req.getPart(s)

  override def upgrade[T <: jakarta.servlet.http.HttpUpgradeHandler](aClass: Class[T]): T =
    req.upgrade(aClass)

  override def getAttribute(s: String): AnyRef = req.getAttribute(s)

  override def getAttributeNames: util.Enumeration[String] = req.getAttributeNames

  override def getCharacterEncoding: String = req.getCharacterEncoding

  override def setCharacterEncoding(s: String): Unit = req.setCharacterEncoding(s)

  override def getContentLength: Int = req.getContentLength

  override def getContentLengthLong: Long = req.getContentLengthLong

  override def getContentType: String = req.getContentType

  override def getInputStream: jakarta.servlet.ServletInputStream = req.getInputStream

  override def getParameter(s: String): String = req.getParameter(s)

  override def getParameterNames: util.Enumeration[String] = req.getParameterNames

  override def getParameterValues(s: String): Array[String] = req.getParameterValues(s)

  override def getParameterMap: util.Map[String, Array[String]] = req.getParameterMap

  override def getProtocol: String = req.getProtocol

  override def getScheme: String = req.getScheme

  override def getServerName: String = req.getServerName

  override def getServerPort: Int = req.getServerPort

  override def getReader: BufferedReader = req.getReader

  override def getRemoteAddr: String = req.getRemoteAddr

  override def getRemoteHost: String = req.getRemoteHost

  override def setAttribute(s: String, o: Any): Unit = req.setAttribute(s, o)

  override def removeAttribute(s: String): Unit = req.removeAttribute(s)

  override def getLocale: Locale = req.getLocale

  override def getLocales: util.Enumeration[Locale] = req.getLocales

  override def isSecure: Boolean = req.isSecure

  override def getRequestDispatcher(s: String): jakarta.servlet.RequestDispatcher =
    req.getRequestDispatcher(s)

  override def getRealPath(s: String): String = req.getRealPath(s)

  override def getRemotePort: Int = req.getRemotePort

  override def getLocalName: String = req.getLocalName

  override def getLocalAddr: String = req.getLocalAddr

  override def getLocalPort: Int = req.getLocalPort

  override def getServletContext: jakarta.servlet.ServletContext = req.getServletContext

  override def startAsync(): jakarta.servlet.AsyncContext = req.startAsync()

  override def startAsync(
      servletRequest: jakarta.servlet.ServletRequest,
      servletResponse: jakarta.servlet.ServletResponse): jakarta.servlet.AsyncContext =
    req.startAsync(servletRequest, servletResponse)

  override def isAsyncStarted: Boolean = req.isAsyncStarted

  override def isAsyncSupported: Boolean = req.isAsyncSupported

  override def getAsyncContext: jakarta.servlet.AsyncContext = req.getAsyncContext

  override def getDispatcherType: jakarta.servlet.DispatcherType = req.getDispatcherType
}

trait HttpServletRequestLike {

  def getParameter(name: String): String

  def getParameterMap: util.Map[String, Array[String]]
}

object HttpServletRequestLike {
  def fromJavax(req: javax.servlet.http.HttpServletRequest): JavaxHttpServletRequest = {
    new JavaxHttpServletRequest(req)
  }

  def fromJakarta(req: jakarta.servlet.http.HttpServletRequest): JakartaHttpServletRequest = {
    new JakartaHttpServletRequest(req)
  }
}
