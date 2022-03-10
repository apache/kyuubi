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

import java.io.UnsupportedEncodingException
import java.nio.ByteBuffer
import java.nio.charset.IllegalCharsetNameException

import org.ietf.jgss.{GSSException, Oid}

/**
 * Copy from Apache Hadoop `KerberosUtil`
 */
object KerberosUtil {
  // Oid for SPNego GSS-API mechanism.
  val GSS_SPNEGO_MECH_OID: Oid = getNumericOidInstance("1.3.6.1.5.5.2")
  // This Oid for Kerberos GSS-API mechanism.
  val GSS_KRB5_MECH_OID: Oid = getNumericOidInstance("1.2.840.113554.1.2.2")
  // Oid for kerberos principal name
  val NT_GSS_KRB5_PRINCIPAL_OID: Oid = getNumericOidInstance("1.2.840.113554.1.2.2.1")

  // numeric oids will never generate a GSSException for a malformed oid.
  // use to initialize statics.
  private def getNumericOidInstance(oidName: String): Oid =
    try {
      new Oid(oidName)
    } catch {
      case ex: GSSException =>
        throw new IllegalArgumentException(ex)
    }

  // basic ASN.1 DER decoder to traverse encoded byte arrays.
  private class DER(val srcbb: ByteBuffer) extends java.util.Iterator[DER] {
    import DER._

    final private val tag: Int = srcbb.get() & 0xFF
    val length = readLength(srcbb)
    final private val bb: ByteBuffer = srcbb.slice
    bb.limit(length)
    srcbb.position(srcbb.position() + length)

    def this(buf: Array[Byte]) {
      this(ByteBuffer.wrap(buf))
    }

    def getTag: Int = {
      tag
    }

    def choose(subtag: Int): DER = {
      while (hasNext) {
        val der: DER = next
        if (der.getTag == subtag) {
          return der
        }
      }
      null
    }

    def get(tags: Int*): DER = {
      var der: DER = this
      for (i <- 0 until tags.length) {
        val expectedTag: Int = tags(i)
        // lookup for exact match, else scan if it's sequenced.
        if (der.getTag != expectedTag) {
          der =
            if (der.hasNext) {
              der.choose(expectedTag)
            } else {
              null
            }
        }
        if (der == null) {
          val sb: StringBuilder = new StringBuilder("Tag not found:")
          for (ii <- 0 to i) {
            sb.append(" 0x").append(Integer.toHexString(tags(ii)))
          }
          throw new IllegalStateException(sb.toString)
        }
      }
      der
    }

    def getAsString: String =
      try {
        new String(bb.array, bb.arrayOffset + bb.position(), bb.remaining, "UTF-8")
      } catch {
        case _: UnsupportedEncodingException =>
          throw new IllegalCharsetNameException("UTF-8") // won't happen.
      }

    override def hashCode: Int = {
      31 * tag + bb.hashCode
    }

    override def equals(o: Any): Boolean = {
      o.isInstanceOf[DER] && tag.equals(o.asInstanceOf[DER].tag) &&
      bb.equals(o.asInstanceOf[DER].bb)
    }

    override def hasNext: Boolean = {
      // it's a sequence or an embedded octet.
      ((tag & 0x30) != 0 || tag == 0x04) && bb.hasRemaining
    }

    override def next: DER = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      new DER(bb)
    }

    override def toString: String = {
      "[tag=0x" + Integer.toHexString(tag) + " bb=" + bb + "]"
    }
  }

  private object DER {
    val SPNEGO_MECH_OID: DER = getDER(GSS_SPNEGO_MECH_OID)
    val KRB5_MECH_OID: DER = getDER(GSS_KRB5_MECH_OID)

    private def getDER(oid: Oid): DER =
      try {
        new DER(oid.getDER)
      } catch {
        case ex: GSSException =>
          // won't happen.  a proper OID is encodable.
          throw new IllegalArgumentException(ex)
      }

    // standard ASN.1 encoding.
    private def readLength(bb: ByteBuffer): Int = {
      var length: Int = bb.get
      if ((length & 0x80.toByte) != 0) {
        val varlength: Int = length & 0x7F
        length = 0
        (0 until varlength).foreach { _ =>
          length = (length << 8) | (bb.get & 0xFF)
        }
      }
      length
    }
  }

  /**
   * Extract the TGS server principal from the given gssapi kerberos or spnego
   * wrapped token.
   *
   * @param rawToken bytes of the gss token
   * @return String of server principal
   * @throws IllegalArgumentException if token is undecodable
   */
  def getTokenServerName(rawToken: Array[Byte]): String = {
    // subsequent comments include only relevant portions of the kerberos
    // DER encoding that will be extracted.
    var token = new DER(rawToken)
    // InitialContextToken ::= [APPLICATION 0] IMPLICIT SEQUENCE {
    //     mech   OID
    //     mech-token  (NegotiationToken or InnerContextToken)
    // }
    var oid = token.next
    if (oid.equals(DER.SPNEGO_MECH_OID)) {
      // NegotiationToken ::= CHOICE {
      //     neg-token-init[0] NegTokenInit
      // NegTokenInit ::= SEQUENCE {
      //     mech-token[2]     InitialContextToken
      token = token.next.get(0xA0, 0x30, 0xA2, 0x04).next
      oid = token.next
    }
    if (!oid.equals(DER.KRB5_MECH_OID)) {
      throw new IllegalArgumentException("Malformed gss token")
    }
    // InnerContextToken ::= {
    //     token-id[1]
    //     AP-REQ
    if (token.next.getTag != 1) throw new IllegalArgumentException("Not an AP-REQ token")
    // AP-REQ ::= [APPLICATION 14] SEQUENCE {
    //     ticket[3]      Ticket
    val ticket = token.next.get(0x6E, 0x30, 0xA3, 0x61, 0x30)
    // Ticket ::= [APPLICATION 1] SEQUENCE {
    //     realm[1]       String
    //     sname[2]       PrincipalName
    // PrincipalName ::= SEQUENCE {
    //     name-string[1] SEQUENCE OF String
    val realm = ticket.get(0xA1, 0x1B).getAsString
    val names = ticket.get(0xA2, 0x30, 0xA1, 0x30)
    val sb = new StringBuilder
    while (names.hasNext) {
      if (sb.length > 0) {
        sb.append('/')
      }
      sb.append(names.next.getAsString)
    }
    sb.append('@').append(realm).toString
  }
}
