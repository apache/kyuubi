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

package org.apache.kyuubi.tpcds.benchmark

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.language.implicitConversions
import scala.util.{Failure => SFailure, Success, Try}
import scala.util.control.NonFatal

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

// scalastyle:off
/**
 * A collection of queries that test a particular aspect of Spark SQL.
 */
abstract class Benchmark(
    @transient val sparkSession: SparkSession)
  extends Serializable {

  import Benchmark._

  val resultsLocation: String = sparkSession.conf.get("spark.sql.perf.results")

  protected def sparkContext = sparkSession.sparkContext

  implicit protected def toOption[A](a: A): Option[A] = Option(a)

  val buildInfo: Map[String, String] =
    Try(getClass.getClassLoader.loadClass("org.apache.spark.BuildInfo")).map { cls =>
      cls.getMethods
        .filter(_.getReturnType == classOf[String])
        .filterNot(_.getName == "toString")
        .map(m => m.getName -> m.invoke(cls).asInstanceOf[String])
        .toMap
    }.getOrElse(Map.empty)

  def currentConfiguration: BenchmarkConfiguration = BenchmarkConfiguration(
    sqlConf = sparkSession.conf.getAll,
    sparkConf = sparkContext.getConf.getAll.toMap,
    defaultParallelism = sparkContext.defaultParallelism,
    buildInfo = buildInfo)

  /**
   * Starts an experiment run with a given set of executions to run.
   *
   * @param executionsToRun a list of executions to run.
   * @param includeBreakdown If it is true, breakdown results of an execution will be recorded.
   *                         Setting it to true may significantly increase the time used to
   *                         run an execution.
   * @param iterations The number of iterations to run of each execution.
   * @param variations [[Variation]]s used in this run.  The cross product of all variations will be
   *                   run for each execution * iteration.
   * @param tags Tags of this run.
   * @param timeout wait at most timeout milliseconds for each query, 0 means wait forever
   * @return It returns a ExperimentStatus object that can be used to
   *         track the progress of this experiment run.
   */
  def runExperiment(
      executionsToRun: Seq[Benchmarkable],
      includeBreakdown: Boolean = false,
      iterations: Int = 3,
      variations: Seq[Variation[_]] = Seq(Variation("StandardRun", Seq("true")) { _ => {} }),
      tags: Map[String, String] = Map.empty,
      timeout: Long = 0L,
      resultPath: String = resultsLocation,
      forkThread: Boolean = true): ExperimentStatus = {

    new ExperimentStatus(
      executionsToRun,
      includeBreakdown,
      iterations,
      variations,
      tags,
      timeout,
      resultPath,
      sparkSession,
      currentConfiguration,
      forkThread = forkThread)
  }

  /** Factory object for benchmark queries. */
  case object Query {
    def apply(
        name: String,
        sqlText: String,
        description: String,
        executionMode: ExecutionMode = ExecutionMode.ForeachResults): Query = {
      new Query(name, sparkSession.sql(sqlText), description, Some(sqlText), executionMode)
    }
  }
}

/**
 * A Variation represents a setting (e.g. the number of shuffle partitions or if tables
 * are cached in memory) that we want to change in a experiment run.
 * A Variation has three parts, `name`, `options`, and `setup`.
 * The `name` is the identifier of a Variation. `options` is a Seq of options that
 * will be used for a query. Basically, a query will be executed with every option
 * defined in the list of `options`. `setup` defines the needed action for every
 * option. For example, the following Variation is used to change the number of shuffle
 * partitions of a query. The name of the Variation is "shufflePartitions". There are
 * two options, 200 and 2000. The setup is used to set the value of property
 * "spark.sql.shuffle.partitions".
 *
 * {{{
 *   Variation("shufflePartitions", Seq("200", "2000")) {
 *     case num => sqlContext.setConf("spark.sql.shuffle.partitions", num)
 *   }
 * }}}
 */
case class Variation[T](name: String, options: Seq[T])(val setup: T => Unit)

case class Table(
    name: String,
    data: Dataset[_])

object Benchmark {

  class ExperimentStatus(
      executionsToRun: Seq[Benchmarkable],
      includeBreakdown: Boolean,
      iterations: Int,
      variations: Seq[Variation[_]],
      tags: Map[String, String],
      timeout: Long,
      val resultPath: String,
      sparkSession: SparkSession,
      currentConfiguration: BenchmarkConfiguration,
      forkThread: Boolean = true) {
    val currentResults = new collection.mutable.ArrayBuffer[BenchmarkResult]()
    val currentRuns = new collection.mutable.ArrayBuffer[ExperimentRun]()
    val currentMessages = new collection.mutable.ArrayBuffer[String]()

    def logMessage(msg: String): Unit = {
      println(msg)
      currentMessages += msg
    }

    // Stats for HTML status message.
    @volatile var currentExecution = ""
    @volatile var currentPlan = "" // for queries only
    @volatile var currentConfig = ""
    @volatile var failures = 0
    @volatile var startTime = 0L

    /** An optional log collection task that will run after the experiment. */
    @volatile var logCollection: () => Unit = () => {}

    def cartesianProduct[T](xss: List[List[T]]): List[List[T]] = xss match {
      case Nil => List(Nil)
      case h :: t => for (xh <- h; xt <- cartesianProduct(t)) yield xh :: xt
    }

    val timestamp: Long = System.currentTimeMillis()
    val combinations: Seq[List[Int]] =
      cartesianProduct(variations.map(l => l.options.indices.toList).toList)
    val resultsFuture: Future[Unit] = Future {
      // Run the benchmarks!
      val results: Seq[ExperimentRun] = (1 to iterations).flatMap { i =>
        combinations.map { setup =>
          val currentOptions = variations.asInstanceOf[Seq[Variation[Any]]].zip(setup).map {
            case (v, idx) =>
              v.setup(v.options(idx))
              v.name -> v.options(idx).toString
          }
          currentConfig = currentOptions.map { case (k, v) => s"$k: $v" }.mkString(", ")

          val res = executionsToRun.flatMap { q =>
            val setup =
              s"iteration: $i, ${currentOptions.map { case (k, v) => s"$k=$v" }.mkString(", ")}"
            logMessage(s"Running execution ${q.name} $setup")

            currentExecution = q.name
            currentPlan = q match {
              case query: Query =>
                try {
                  query.newDataFrame().queryExecution.executedPlan.toString()
                } catch {
                  case e: Exception =>
                    s"failed to parse: $e"
                }
              case _ => ""
            }
            startTime = System.currentTimeMillis()

            val singleResultT = Try {
              q.benchmark(
                includeBreakdown,
                setup,
                currentMessages,
                timeout,
                forkThread = forkThread)
            }

            singleResultT match {
              case Success(singleResult) =>
                singleResult.failure.foreach { f =>
                  failures += 1
                  logMessage(s"Execution '${q.name}' failed: ${f.message}")
                }
                singleResult.executionTime.foreach { time =>
                  logMessage(s"Execution time: ${time / 1000}s")
                }
                currentResults += singleResult
                singleResult :: Nil
              case SFailure(e) =>
                failures += 1
                logMessage(s"Execution '${q.name}' failed: ${e}")
                Nil
            }
          }

          val result = ExperimentRun(
            timestamp = timestamp,
            iteration = i,
            tags = currentOptions.toMap ++ tags,
            configuration = currentConfiguration,
            res)

          currentRuns += result

          result
        }
      }

      try {
        val resultsTable = sparkSession.createDataFrame(results)
        logMessage(s"Results written to table: 'sqlPerformance' at $resultPath")
        resultsTable
          .coalesce(1)
          .write
          .format("json")
          .save(resultPath)
      } catch {
        case NonFatal(e) =>
          logMessage(s"Failed to write data: $e")
          throw e
      }

      logCollection()
    }

    /** Waits for the finish of the experiment. */
    def waitForFinish(timeoutInSeconds: Int): Unit = {
      Await.result(resultsFuture, timeoutInSeconds.seconds)
    }

    /** Returns full iterations from an actively running experiment. */
    def getCurrentRuns(): DataFrame = {
      val tbl = sparkSession.createDataFrame(currentRuns)
      tbl.createOrReplaceTempView("currentRuns")
      tbl
    }

    override def toString: String =
      s"""Permalink: table("sqlPerformance").where('timestamp === ${timestamp}L)"""
  }
}
// scalastyle:on
