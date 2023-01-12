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

package org.apache.kyuubi.engine.spark.repl

import java.io.{ByteArrayOutputStream, File}
import java.util.concurrent.locks.ReentrantLock

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.IR
import scala.tools.nsc.interpreter.JPrintWriter

import org.apache.spark.SparkContext
import org.apache.spark.repl.SparkILoop
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.MutableURLClassLoader

private[spark] case class KyuubiSparkILoop private (
    spark: SparkSession,
    output: ByteArrayOutputStream)
  extends SparkILoop(None, new JPrintWriter(output)) {
  import KyuubiSparkILoop._

  val result = new DataFrameHolder(spark)

  private def initialize(): Unit = withLockRequired {
    settings = new Settings
    val interpArguments = List(
      "-Yrepl-class-based",
      "-Yrepl-outdir",
      s"${spark.sparkContext.getConf.get("spark.repl.class.outputDir")}")
    settings.processArguments(interpArguments, processAll = true)
    settings.usejavacp.value = true
    val currentClassLoader = Thread.currentThread().getContextClassLoader
    settings.embeddedDefaults(currentClassLoader)
    this.createInterpreter()
    this.initializeSynchronous()
    try {
      this.compilerClasspath
      this.ensureClassLoader()
      var classLoader: ClassLoader = Thread.currentThread().getContextClassLoader
      while (classLoader != null) {
        classLoader match {
          case loader: MutableURLClassLoader =>
            val allJars = loader.getURLs.filter { u =>
              val file = new File(u.getPath)
              u.getProtocol == "file" && file.isFile &&
              file.getName.contains("scala-lang_scala-reflect")
            }
            this.addUrlsToClassPath(allJars: _*)
            classLoader = null
          case _ =>
            classLoader = classLoader.getParent
        }
      }

      this.addUrlsToClassPath(
        classOf[DataFrameHolder].getProtectionDomain.getCodeSource.getLocation)
    } finally {
      Thread.currentThread().setContextClassLoader(currentClassLoader)
    }

    this.beQuietDuring {
      // SparkSession/SparkContext and their implicits
      this.bind("spark", classOf[SparkSession].getCanonicalName, spark, List("""@transient"""))
      this.bind(
        "sc",
        classOf[SparkContext].getCanonicalName,
        spark.sparkContext,
        List("""@transient"""))

      this.interpret("import org.apache.spark.SparkContext._")
      this.interpret("import spark.implicits._")
      this.interpret("import spark.sql")
      this.interpret("import org.apache.spark.sql.functions._")

      // for feeding results to client, e.g. beeline
      this.bind(
        "result",
        classOf[DataFrameHolder].getCanonicalName,
        result)
    }
  }

  def getResult(statementId: String): DataFrame = result.get(statementId)

  def clearResult(statementId: String): Unit = result.unset(statementId)

  def interpretWithRedirectOutError(statement: String): IR.Result = withLockRequired {
    Console.withOut(output) {
      Console.withErr(output) {
        this.interpret(statement)
      }
    }
  }

  def getOutput: String = {
    val res = output.toString.trim
    output.reset()
    res
  }
}

private[spark] object KyuubiSparkILoop {
  def apply(spark: SparkSession): KyuubiSparkILoop = {
    val os = new ByteArrayOutputStream()
    val iLoop = new KyuubiSparkILoop(spark, os)
    iLoop.initialize()
    iLoop
  }

  private val lock = new ReentrantLock()
  private def withLockRequired[T](block: => T): T = {
    try {
      lock.lock()
      block
    } finally lock.unlock()
  }
}
