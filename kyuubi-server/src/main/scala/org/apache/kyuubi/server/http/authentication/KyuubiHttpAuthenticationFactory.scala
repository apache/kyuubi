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

package org.apache.kyuubi.server.http.authentication

import java.security.PrivilegedAction
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import com.codahale.metrics.jetty9.InstrumentedHandler
import org.apache.hadoop.security.UserGroupInformation
import org.eclipse.jetty.server.{Handler, Request}
import org.eclipse.jetty.server.handler.HandlerWrapper

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{AUTHENTICATION_METHOD, ENGINE_SECURITY_ENABLED}
import org.apache.kyuubi.metrics.MetricsConstants.{REST_CONN_FAIL, REST_CONN_OPEN, REST_CONN_TOTAL}
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.service.authentication.{AuthTypes, InternalSecurityAccessor}
import org.apache.kyuubi.service.authentication.AuthTypes.KERBEROS

class KyuubiHttpAuthenticationFactory(conf: KyuubiConf) {
  private val authTypes = conf.get(AUTHENTICATION_METHOD).map(AuthTypes.withName)
  private val kerberosEnabled = authTypes.contains(KERBEROS)
  private val ugi = UserGroupInformation.getCurrentUser

  if (conf.get(ENGINE_SECURITY_ENABLED)) {
    InternalSecurityAccessor.initialize(conf, true)
  }

  private[kyuubi] val httpHandlerWrapperFactory =
    new HttpHandlerWrapperFactory(ugi, kerberosEnabled)

  class HttpHandlerWrapperFactory(ugi: UserGroupInformation, kerberosEnabled: Boolean) {
    def wrapHandler(handler: Handler, metricPrefix: Option[String] = None): HandlerWrapper = {
      val handlerWrapper = new HandlerWrapper {
        _handler = handler

        override def handle(
            target: String,
            baseRequest: Request,
            request: HttpServletRequest,
            response: HttpServletResponse): Unit = {
          MetricsSystem.tracing { ms =>
            ms.incCount(REST_CONN_TOTAL)
            ms.incCount(REST_CONN_OPEN)
          }
          try {
            if (kerberosEnabled) {
              ugi.doAs(new PrivilegedAction[Unit] {
                override def run(): Unit = {
                  handler.handle(target, baseRequest, request, response)
                }
              })
            } else {
              handler.handle(target, baseRequest, request, response)
            }
          } finally {
            val statusCode = response.getStatus
            if (statusCode < 200 || statusCode >= 300) {
              MetricsSystem.tracing { ms =>
                ms.incCount(REST_CONN_FAIL)
              }
            }
            MetricsSystem.tracing { ms =>
              ms.decCount(REST_CONN_OPEN)
            }
            AuthenticationFilter.HTTP_CLIENT_USER_NAME.remove()
            AuthenticationFilter.HTTP_CLIENT_IP_ADDRESS.remove()
            AuthenticationFilter.HTTP_PROXY_HEADER_CLIENT_IP_ADDRESS.remove()
            AuthenticationFilter.HTTP_AUTH_TYPE.remove()
            AuthenticationFilter.HTTP_CLIENT_PROXY_USER_NAME.remove()
            AuthenticationFilter.HTTP_FORWARDED_ADDRESSES.remove()
          }
        }

        override def doStart(): Unit = {
          super.doStart()
          handler.start()
        }
      }

      (MetricsSystem.getMetricsRegistry, metricPrefix) match {
        case (Some(metricRegistry), Some(prefix)) =>
          val instrumentedHandler = new InstrumentedHandler(metricRegistry, prefix)
          instrumentedHandler.setHandler(handlerWrapper)
          instrumentedHandler
        case _ => handlerWrapper
      }
    }
  }
}
