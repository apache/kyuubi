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

package org.apache.kyuubi.sql.parser

import java.lang.{Long => JLong}
import java.nio.CharBuffer

import org.antlr.v4.runtime.{CharStream, CodePointCharStream, IntStream}
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.{AbstractParseTreeVisitor, ParseTree}

import org.apache.kyuubi.sql.KyuubiSqlBaseParserBaseListener
import org.apache.kyuubi.sql.plan.KyuubiTreeNode

abstract class KyuubiParserBase[P] {

  val astBuilder: AbstractParseTreeVisitor[AnyRef]

  protected def parse[T](command: String)(toResult: P => T): T

  def parseTree(parser: P): ParseTree

  def parsePlan(sqlText: String): KyuubiTreeNode = parse(sqlText) { parser =>
    astBuilder.visit(parseTree(parser)) match {
      case plan: KyuubiTreeNode => plan
    }
  }
}

object KyuubiParser {
  val U16_CHAR_PATTERN = """\\u([a-fA-F0-9]{4})(?s).*""".r
  val U32_CHAR_PATTERN = """\\U([a-fA-F0-9]{8})(?s).*""".r
  val OCTAL_CHAR_PATTERN = """\\([01][0-7]{2})(?s).*""".r
  val ESCAPED_CHAR_PATTERN = """\\((?s).)(?s).*""".r

  /** Unescape backslash-escaped string enclosed by quotes. */
  def unescapeSQLString(b: String): String = {
    val sb = new StringBuilder(b.length())

    def appendEscapedChar(n: Char): Unit = {
      n match {
        case '0' => sb.append('\u0000')
        case '\'' => sb.append('\'')
        case '"' => sb.append('\"')
        case 'b' => sb.append('\b')
        case 'n' => sb.append('\n')
        case 'r' => sb.append('\r')
        case 't' => sb.append('\t')
        case 'Z' => sb.append('\u001A')
        case '\\' => sb.append('\\')
        // The following 2 lines are exactly what MySQL does TODO: why do we do this?
        case '%' => sb.append("\\%")
        case '_' => sb.append("\\_")
        case _ => sb.append(n)
      }
    }

    if (b.startsWith("r") || b.startsWith("R")) {
      b.substring(2, b.length - 1)
    } else {
      // Skip the first and last quotations enclosing the string literal.
      val charBuffer = CharBuffer.wrap(b, 1, b.length - 1)

      while (charBuffer.remaining() > 0) {
        charBuffer match {
          case U16_CHAR_PATTERN(cp) =>
            // \u0000 style 16-bit unicode character literals.
            sb.append(Integer.parseInt(cp, 16).toChar)
            charBuffer.position(charBuffer.position() + 6)
          case U32_CHAR_PATTERN(cp) =>
            // \U00000000 style 32-bit unicode character literals.
            // Use Long to treat codePoint as unsigned in the range of 32-bit.
            val codePoint = JLong.parseLong(cp, 16)
            if (codePoint < 0x10000) {
              sb.append((codePoint & 0xFFFF).toChar)
            } else {
              val highSurrogate = (codePoint - 0x10000) / 0x400 + 0xD800
              val lowSurrogate = (codePoint - 0x10000) % 0x400 + 0xDC00
              sb.append(highSurrogate.toChar)
              sb.append(lowSurrogate.toChar)
            }
            charBuffer.position(charBuffer.position() + 10)
          case OCTAL_CHAR_PATTERN(cp) =>
            // \000 style character literals.
            sb.append(Integer.parseInt(cp, 8).toChar)
            charBuffer.position(charBuffer.position() + 4)
          case ESCAPED_CHAR_PATTERN(c) =>
            // escaped character literals.
            appendEscapedChar(c.charAt(0))
            charBuffer.position(charBuffer.position() + 2)
          case _ =>
            // non-escaped character literals.
            sb.append(charBuffer.get())
        }
      }
      sb.toString()
    }
  }
}

case object PostProcessor extends KyuubiSqlBaseParserBaseListener {}

/* Copied from Apache Spark's to avoid dependency on Spark Internals */
class UpperCaseCharStream(wrapped: CodePointCharStream) extends CharStream {
  override def consume(): Unit = wrapped.consume()
  override def getSourceName(): String = wrapped.getSourceName
  override def index(): Int = wrapped.index
  override def mark(): Int = wrapped.mark
  override def release(marker: Int): Unit = wrapped.release(marker)
  override def seek(where: Int): Unit = wrapped.seek(where)
  override def size(): Int = wrapped.size

  override def getText(interval: Interval): String = wrapped.getText(interval)

  // scalastyle:off
  override def LA(i: Int): Int = {
    val la = wrapped.LA(i)
    if (la == 0 || la == IntStream.EOF) la
    else Character.toUpperCase(la)
  }
  // scalastyle:on
}
