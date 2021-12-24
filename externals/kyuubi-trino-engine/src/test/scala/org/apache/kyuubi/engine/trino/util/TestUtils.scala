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

package org.apache.kyuubi.engine.trino.util

import java.util.Optional

import scala.collection.JavaConverters._

import io.trino.client.ClientStandardTypes._
import io.trino.client.ClientTypeSignature
import io.trino.client.ClientTypeSignatureParameter
import io.trino.client.Column
import io.trino.client.NamedClientTypeSignature
import io.trino.client.RowFieldName

object TestUtils {

  lazy val decimalTypeSignature: ClientTypeSignature = new ClientTypeSignature(
    DECIMAL,
    List(
      ClientTypeSignatureParameter.ofLong(10),
      ClientTypeSignatureParameter.ofLong(8)).asJava)

  lazy val arrayTypeSignature: ClientTypeSignature = new ClientTypeSignature(
    ARRAY,
    List(ClientTypeSignatureParameter.ofType(new ClientTypeSignature(DOUBLE))).asJava)

  lazy val mapTypeSignature: ClientTypeSignature = new ClientTypeSignature(
    MAP,
    List(
      ClientTypeSignatureParameter.ofType(new ClientTypeSignature(INTEGER)),
      ClientTypeSignatureParameter.ofType(new ClientTypeSignature(DOUBLE))).asJava)

  lazy val rowTypeSignature: ClientTypeSignature = new ClientTypeSignature(
    ROW,
    List(
      ClientTypeSignatureParameter.ofNamedType(
        new NamedClientTypeSignature(
          Optional.of(new RowFieldName("foo")),
          new ClientTypeSignature(VARCHAR))),
      ClientTypeSignatureParameter.ofNamedType(
        new NamedClientTypeSignature(
          Optional.of(new RowFieldName("bar")),
          mapTypeSignature))).asJava)

  lazy val textTypeSignature: ClientTypeSignature = new ClientTypeSignature("text")

  def column(name: String, tp: String): Column = column(name, tp, new ClientTypeSignature(tp))

  def column(name: String, tp: String, signature: ClientTypeSignature): Column = {
    new Column(name, tp, signature)
  }
}
