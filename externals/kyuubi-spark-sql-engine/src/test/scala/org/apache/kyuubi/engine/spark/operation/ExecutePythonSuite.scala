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

package org.apache.kyuubi.engine.spark.operation

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.Try

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiReservedKeys.{KYUUBI_SESSION_USER_KEY, KYUUBI_STATEMENT_ID_KEY}
import org.apache.kyuubi.engine.spark.KyuubiSparkUtil.SPARK_ENGINE_RUNTIME_VERSION
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.operation.OperationHandle
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion
import org.apache.kyuubi.tags.PySparkTest

@PySparkTest
class ExecutePythonSuite extends WithSparkSQLEngine {

  override def withKyuubiConf: Map[String, String] = Map.empty

  /**
   * Every python operation of a session runs on the worker's pinned thread, so a bare `runCode`
   * outside any operation observes exactly the Spark local properties that the last operation's
   * teardown left on that thread.
   */
  private def probe(worker: SessionPythonWorker, expr: String): String = {
    val response = worker.runCode(s"print(repr($expr))")
    assert(response.isDefined, s"no response for $expr")
    val content = response.get.content
    assert(
      content.status === PythonResponse.OK_STATUS,
      s"probe failed: ${content.getEname()}: ${content.getEvalue()}")
    content.data.getOrElse("text/plain", "").toString.trim
  }

  /**
   * The version of the PySpark that the worker imports, resolved the way
   * `createSessionPythonWorker` resolves `SPARK_HOME` and `execute_python.py` extends `sys.path`.
   * `None` when it cannot be read, so that only a known mismatch cancels a test.
   */
  private lazy val workerPySparkVersion: Option[String] =
    Try(sys.env.getOrElse("SPARK_HOME", ExecutePython.defaultSparkHome)).toOption
      .map(Paths.get(_, "python", "pyspark", "version.py"))
      .filter(Files.exists(_))
      .flatMap { path =>
        """__version__[^'"]*["']([^'"]+)["']""".r
          .findFirstMatchIn(new String(Files.readAllBytes(path), UTF_8)).map(_.group(1))
      }

  private def withSessionAndWorker(f: (Session, SessionPythonWorker) => Unit): Unit = {
    // The worker's PySpark talks to this JVM's Spark classes over Py4J, so it cannot attach to a
    // different Spark, as in the CI jobs that run a Spark 3.5 engine on a Spark 4.x binary.
    assume(
      workerPySparkVersion.forall(SPARK_ENGINE_RUNTIME_VERSION === _),
      s"pyspark ${workerPySparkVersion.getOrElse("")} under SPARK_HOME cannot attach to" +
        s" Spark $SPARK_ENGINE_RUNTIME_VERSION")

    val sessionManager = engine.backendService.sessionManager
    val handle = sessionManager.openSession(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11,
      Utils.currentUser,
      "",
      "127.0.0.1",
      Map.empty)
    val session = sessionManager.getSession(handle)
    ExecutePython.init()
    val worker = ExecutePython.createSessionPythonWorker(spark, session)
    try {
      f(session, worker)
    } finally {
      worker.close()
      sessionManager.closeSession(handle)
    }
  }

  test("KYUUBI #7684 - the operation context is cleared with null, not the empty string") {
    withSessionAndWorker { (session, worker) =>
      val operation =
        new ExecutePython(session, "print(1)", false, 0L, worker, OperationHandle())
      operation.run()

      // An empty string here passes the null check in AuthZUtils.getAuthzUgi and reaches
      // UserGroupInformation.createRemoteUser, which rejects it with `Null user`.
      assert(probe(worker, s"spark.sparkContext.getLocalProperty('$KYUUBI_SESSION_USER_KEY')")
        === "None")
      assert(probe(worker, s"spark.sparkContext.getLocalProperty('$KYUUBI_STATEMENT_ID_KEY')")
        === "None")
    }
  }

  test("KYUUBI #7684 - the operation context is atomic with respect to the shared worker") {
    withSessionAndWorker { (session, worker) =>
      val operation = new ProbeExecutePython(session, worker)
      val otherOperationRanCode = new AtomicBoolean(false)
      val other = new Thread(new Runnable {
        override def run(): Unit = {
          worker.runCode("1")
          otherOperationRanCode.set(true)
        }
      })

      operation.withContext {
        // The context of this operation is applied. Another operation of the same session shares
        // this worker, so if it can run code here it can also set or clear these properties here.
        other.start()
        other.join(TimeUnit.SECONDS.toMillis(5))
        assert(
          !otherOperationRanCode.get(),
          "another operation reached the worker while this operation held the context")
      }

      other.join(TimeUnit.SECONDS.toMillis(10))
    }
  }
}

/**
 * Gives a test access to the operation context without running a python statement, so that the
 * body of `withLocalProperties` does not take the worker lock on its own.
 */
private class ProbeExecutePython(session: Session, worker: SessionPythonWorker)
  extends ExecutePython(session, "print(1)", false, 0L, worker, OperationHandle()) {
  def withContext[T](f: => T): T = withLocalProperties(f)
}
