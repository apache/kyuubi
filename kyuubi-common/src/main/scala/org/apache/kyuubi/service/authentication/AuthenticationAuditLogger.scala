package org.apache.kyuubi.service.authentication

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.service.authentication.AuthenticationFilter.{getForwardedAddresses, HTTP_AUTH_TYPE, HTTP_CLIENT_IP_ADDRESS, HTTP_CLIENT_PROXY_USER_NAME, HTTP_CLIENT_USER_NAME, HTTP_FORWARDED_ADDRESSES, HTTP_PROXY_HEADER_CLIENT_IP_ADDRESS}

object AuthenticationAuditLogger extends Logging {
  final private val AUDIT_BUFFER = new ThreadLocal[StringBuilder]() {
    override protected def initialValue: StringBuilder = new StringBuilder()
  }

  def audit(request: HttpServletRequest, response: HttpServletResponse): Unit = {
    val sb = AUDIT_BUFFER.get()
    sb.setLength(0)
    sb.append(s"user=${HTTP_CLIENT_USER_NAME.get()}(auth:${HTTP_AUTH_TYPE.get()})").append("\t")
    if (HTTP_CLIENT_PROXY_USER_NAME.get() != null) {
      sb.append(s"proxyUser=${HTTP_CLIENT_PROXY_USER_NAME.get()}").append("\t")
    }
    sb.append(s"ip=${HTTP_CLIENT_IP_ADDRESS.get()}").append("\t")
    sb.append(s"proxyIp=${HTTP_PROXY_HEADER_CLIENT_IP_ADDRESS.get()}").append("\t")
    if (HTTP_FORWARDED_ADDRESSES.get().nonEmpty) {
      sb.append(s"forwardedFor=${getForwardedAddresses.mkString(",")}").append("\t")
    }
    sb.append(s"method=${request.getMethod}").append("\t")
    sb.append(s"uri=${request.getRequestURI}").append("\t")
    sb.append(s"params=${request.getQueryString}").append("\t")
    sb.append(s"protocol=${request.getProtocol}").append("\t")
    sb.append(s"status=${response.getStatus}")
    info(sb.toString())
  }
}
