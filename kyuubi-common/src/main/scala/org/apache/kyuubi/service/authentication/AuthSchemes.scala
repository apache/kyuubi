package org.apache.kyuubi.service.authentication

object AuthSchemes extends Enumeration {
  type AuthScheme = Value

  val BASIC, BEARER, NEGOTIATE, KYUUBI_INTERNAL = Value
}
