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

package org.apache.kyuubi.engine.jdbc.util

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.sql.{ResultSet, ResultSetMetaData, Statement}

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.engine.jdbc.schema.Row

class ResultSetFetchIteratorSuite extends KyuubiFunSuite {

  test("hasNext should not skip prefetched metadata rows") {
    val resultSet = newFakeResultSet(Seq("row1", "row2", "row3"))
    val iterator = new ResultSetFetchIterator(resultSet.resultSet)

    assert(iterator.getMetadata.getColumnCount === 1)
    assert(iterator.hasNext)
    assert(iterator.hasNext)
    assert(resultSet.nextCalls === 1)
    assert(iterator.next() === Row(List("row1")))

    assert(iterator.hasNext)
    assert(iterator.hasNext)
    assert(resultSet.nextCalls === 2)
    assert(iterator.next() === Row(List("row2")))

    assert(iterator.hasNext)
    assert(iterator.next() === Row(List("row3")))
    assert(!iterator.hasNext)
    assert(!iterator.hasNext)
    assert(resultSet.nextCalls === 4)
    assert(iterator.getPosition === 3)
  }

  test("close should close the metadata ResultSet and its Statement") {
    val resultSet = newFakeResultSet(Seq("row1"))
    val iterator = new ResultSetFetchIterator(resultSet.resultSet)

    iterator.close()
    iterator.close()

    assert(resultSet.resultSetCloseCalls === 1)
    assert(resultSet.statementCloseCalls === 1)
    assert(!iterator.hasNext)
  }

  private def newFakeResultSet(rows: Seq[Any]): FakeResultSet = {
    new FakeResultSet(rows)
  }

  private class FakeResultSet(rows: Seq[Any]) {
    private var cursor: Int = -1

    var nextCalls: Int = 0
    var resultSetCloseCalls: Int = 0
    var statementCloseCalls: Int = 0

    val metadata: ResultSetMetaData = proxy[ResultSetMetaData] {
      case ("getColumnCount", _) => Integer.valueOf(1)
    }

    val statement: Statement = proxy[Statement] {
      case ("close", _) =>
        statementCloseCalls += 1
        null
    }

    val resultSet: ResultSet = proxy[ResultSet] {
      case ("getMetaData", _) => metadata
      case ("getStatement", _) => statement
      case ("next", _) =>
        nextCalls += 1
        cursor += 1
        Boolean.box(cursor < rows.length)
      case ("getObject", args) =>
        assert(args.toSeq === Seq(Integer.valueOf(1)))
        rows(cursor).asInstanceOf[AnyRef]
      case ("close", _) =>
        resultSetCloseCalls += 1
        null
    }
  }

  private def proxy[A](handler: PartialFunction[(String, Array[AnyRef]), AnyRef])(
      implicit manifest: Manifest[A]): A = {
    Proxy
      .newProxyInstance(
        manifest.runtimeClass.getClassLoader,
        Array(manifest.runtimeClass),
        new InvocationHandler {
          override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
            val safeArgs = Option(args).getOrElse(Array.empty[AnyRef])
            handler.applyOrElse(
              (method.getName, safeArgs),
              defaultReturn(method.getReturnType))
          }
        })
      .asInstanceOf[A]
  }

  private def defaultReturn(returnType: Class[_]): ((String, Array[AnyRef])) => AnyRef = { _ =>
    if (!returnType.isPrimitive || returnType == java.lang.Void.TYPE) {
      null
    } else if (returnType == java.lang.Boolean.TYPE) {
      Boolean.box(false)
    } else if (returnType == java.lang.Byte.TYPE) {
      Byte.box(0.toByte)
    } else if (returnType == java.lang.Short.TYPE) {
      Short.box(0.toShort)
    } else if (returnType == java.lang.Integer.TYPE) {
      Integer.valueOf(0)
    } else if (returnType == java.lang.Long.TYPE) {
      Long.box(0L)
    } else if (returnType == java.lang.Float.TYPE) {
      Float.box(0.0f)
    } else if (returnType == java.lang.Double.TYPE) {
      Double.box(0.0d)
    } else if (returnType == java.lang.Character.TYPE) {
      Character.valueOf(0.toChar)
    } else {
      null
    }
  }
}
