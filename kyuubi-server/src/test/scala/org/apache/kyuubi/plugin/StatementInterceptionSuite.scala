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

package org.apache.kyuubi.plugin

import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import org.apache.kyuubi.{KyuubiFunSuite, KyuubiSQLException}

class StatementInterceptionSuite extends KyuubiFunSuite {

  private def contextFor(statement: String): StatementInterceptContext =
    new StatementInterceptContextImpl(
      "session-1",
      "op-1",
      "alice",
      "alice_real",
      "127.0.0.1",
      statement,
      java.util.Collections.emptyMap[String, String](),
      true,
      0L,
      "SPARK_SQL")

  private def run(interceptors: Seq[StatementInterceptor], statement: String): String =
    StatementInterception.run(interceptors, statement, contextFor)

  private def interceptor(
      f: StatementInterceptContext => StatementInterceptResult): StatementInterceptor =
    new StatementInterceptor {
      override def beforeExecuteStatement(
          context: StatementInterceptContext): StatementInterceptResult = f(context)
    }

  test("no interceptors returns the original statement") {
    assert(run(Nil, "SELECT 1") === "SELECT 1")
  }

  test("PROCEED keeps the statement unchanged") {
    val i = interceptor(_ => StatementInterceptResult.proceed())
    assert(run(Seq(i), "SELECT 1") === "SELECT 1")
  }

  test("REWRITE replaces the statement") {
    val i = interceptor(_ => StatementInterceptResult.rewrite("SELECT 2"))
    assert(run(Seq(i), "SELECT 1") === "SELECT 2")
  }

  test("REJECT throws KyuubiSQLException carrying the message and a policy SQLState") {
    val i = interceptor(_ => StatementInterceptResult.reject("blocked by policy"))
    val e = intercept[KyuubiSQLException](run(Seq(i), "SELECT 1"))
    assert(e.getMessage.contains("blocked by policy"))
    assert(e.getSQLState === "42501")
  }

  test("interceptors run in order and rewrites chain") {
    val appendA = interceptor(ctx => StatementInterceptResult.rewrite(ctx.statement() + " a"))
    val appendB = interceptor(ctx => StatementInterceptResult.rewrite(ctx.statement() + " b"))
    assert(run(Seq(appendA, appendB), "x") === "x a b")
  }

  test("later interceptors see the rewritten statement") {
    var seenBySecond: String = null
    val first = interceptor(_ => StatementInterceptResult.rewrite("REWRITTEN"))
    val second = interceptor { ctx =>
      seenBySecond = ctx.statement()
      StatementInterceptResult.proceed()
    }
    run(Seq(first, second), "ORIGINAL")
    assert(seenBySecond === "REWRITTEN")
  }

  test("the context exposes the statement id") {
    var seenStatementId: String = null
    val i = interceptor { ctx =>
      seenStatementId = ctx.statementId()
      StatementInterceptResult.proceed()
    }
    run(Seq(i), "SELECT 1")
    assert(seenStatementId === "op-1")
  }

  test("the context exposes the effective and real user") {
    var seenUser: String = null
    var seenRealUser: String = null
    val i = interceptor { ctx =>
      seenUser = ctx.user()
      seenRealUser = ctx.realUser()
      StatementInterceptResult.proceed()
    }
    run(Seq(i), "SELECT 1")
    assert(seenUser === "alice")
    assert(seenRealUser === "alice_real")
  }

  test("a throwing interceptor fails the statement") {
    val i = interceptor(_ => throw new RuntimeException("boom"))
    val e = intercept[KyuubiSQLException](run(Seq(i), "SELECT 1"))
    assert(e.getMessage.contains("failed"))
  }

  test("a null result fails the statement") {
    val i = interceptor(_ => null)
    val e = intercept[KyuubiSQLException](run(Seq(i), "SELECT 1"))
    assert(e.getMessage.contains("returned null"))
  }

  test("REJECT short-circuits later interceptors") {
    val rejecting = interceptor(_ => StatementInterceptResult.reject("blocked"))
    val mustNotRun = interceptor(_ => throw new RuntimeException("must not run"))
    val e = intercept[KyuubiSQLException](run(Seq(rejecting, mustNotRun), "SELECT 1"))
    assert(e.getMessage.contains("blocked"))
  }

  test("rewrite/reject reject null or blank arguments") {
    intercept[IllegalArgumentException](StatementInterceptResult.rewrite(null))
    intercept[IllegalArgumentException](StatementInterceptResult.rewrite("  "))
    intercept[IllegalArgumentException](StatementInterceptResult.reject(null))
    intercept[IllegalArgumentException](StatementInterceptResult.reject(""))
  }

  test("initialize closes attempted interceptors in reverse order on failure") {
    val closed = new java.util.concurrent.CopyOnWriteArrayList[String]()
    def okInterceptor(name: String): StatementInterceptor = new StatementInterceptor {
      override def beforeExecuteStatement(
          context: StatementInterceptContext): StatementInterceptResult =
        StatementInterceptResult.proceed()
      override def close(): Unit = closed.add(name)
    }
    val failing = new StatementInterceptor {
      override def initialize(conf: java.util.Map[String, String]): Unit =
        throw new RuntimeException("init failed")
      override def beforeExecuteStatement(
          context: StatementInterceptContext): StatementInterceptResult =
        StatementInterceptResult.proceed()
      override def close(): Unit = closed.add("failing")
    }
    val e = intercept[RuntimeException] {
      StatementInterception.initialize(
        Seq(okInterceptor("a"), okInterceptor("b"), failing),
        java.util.Collections.emptyMap[String, String]())
    }
    assert(e.getMessage.contains("init failed"))
    assert(closed.asScala.toList === List("failing", "b", "a"))
  }

  test("run is thread-safe under concurrent invocation") {
    val appendX = interceptor(ctx => StatementInterceptResult.rewrite(ctx.statement() + "-x"))
    val futures = (1 to 200).map(i => Future(run(Seq(appendX), s"q$i")))
    val results = Await.result(Future.sequence(futures), 30.seconds)
    assert(results.forall(_.endsWith("-x")))
    assert(results.distinct.size === 200)
  }
}
