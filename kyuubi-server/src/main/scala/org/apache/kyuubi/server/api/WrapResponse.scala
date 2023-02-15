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

package org.apache.kyuubi.server.api

import java.io.{ByteArrayOutputStream, OutputStreamWriter, PrintWriter}
import javax.servlet.ServletOutputStream
import javax.servlet.http.{HttpServletResponse, HttpServletResponseWrapper}

class WrapResponse(response: HttpServletResponse) extends HttpServletResponseWrapper(response) {
  var buffer: ByteArrayOutputStream = new ByteArrayOutputStream()
  var output: ServletOutputStream = _
  var writer: PrintWriter = _

  override def getOutputStream: ServletOutputStream = {
    if (writer != null) {
      throw new IllegalStateException("getWriter() has already been called on this response.");
    }
    if (output == null) {
      output = new WrapOutputStream(buffer)
    }
    output
  }

  override def getWriter: PrintWriter = {
    if (output != null) {
      throw new IllegalStateException(
        "getOutputStream() has already been called on this response.");
    }
    if (writer == null) {
      writer = new PrintWriter(new OutputStreamWriter(buffer, getCharacterEncoding()));
    }
    writer
  }

  override def flushBuffer(): Unit = {
    super.flushBuffer()
    if (writer != null) {
      writer.flush();
    } else if (output != null) {
      output.flush();
    }
  }

  def getContent(): String = {
    flushBuffer()
    new String(buffer.toByteArray)
  }
}
