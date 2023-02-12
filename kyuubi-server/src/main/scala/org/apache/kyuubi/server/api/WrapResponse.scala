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

import java.io.{ByteArrayOutputStream, PrintWriter}
import javax.servlet.ServletOutputStream
import javax.servlet.http.{HttpServletResponse, HttpServletResponseWrapper}

class WrapResponse(response: HttpServletResponse) extends HttpServletResponseWrapper(response) {
  var out: ByteArrayOutputStream = new ByteArrayOutputStream()
  var stream: ServletOutputStream = new WrapOutputStream(out)
  var writer: PrintWriter = new PrintWriter(out)

  override def getOutputStream: ServletOutputStream = {
    stream
  }

  override def getWriter: PrintWriter = {
    writer
  }

  def getData: String = {
    if (stream != null) {
      stream.flush()
    }
    if (writer != null) {
      writer.flush()
    }
    out.toString()
  }
}
