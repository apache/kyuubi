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

import java.io.{ByteArrayOutputStream, File, PrintWriter}
import java.util.concurrent.locks.ReentrantLock

import scala.collection.JavaConverters._
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.Results

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ArrayNode
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.repl.SparkILoop
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.MutableURLClassLoader
import org.json4s.DefaultFormats
import org.json4s.Extraction
import org.json4s.JsonAST._
import org.json4s.JsonDSL._

import org.apache.kyuubi.Utils
import org.apache.kyuubi.engine.spark.util.JsonUtils._

private[spark] case class KyuubiSparkILoop private (
    spark: SparkSession,
    output: ByteArrayOutputStream)
  extends SparkILoop(None, new PrintWriter(output)) {
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

  def interpretWithRedirectOutError(statement: String): InterpretResponse = withLockRequired {
    Console.withOut(output) {
      Console.withErr(output) {
        interpretLines(statement.trim.split("\n").toList, InterpretSuccess(TEXT_PLAIN -> ""))
      }
    }
  }

  private def interpretLines(
      lines: List[String],
      resultFromLastLine: InterpretResponse): InterpretResponse = {
    lines match {
      case Nil => resultFromLastLine
      case head :: tail =>
        val result = interpretLine(head)

        result match {
          case InterpretInComplete() =>
            tail match {
              case Nil =>
                // InterpretInComplete could be caused by an actual incomplete statements
                // (e.g. "sc.") or statements with just comments.
                // To distinguish them, reissue the same statement wrapped in { }.
                // If it is an actual incomplete statement, the interpreter will return an error.
                // If it is some comment, the interpreter will return success.
                interpretLine(s"{\n$head\n}") match {
                  case InterpretInComplete() | InterpretError(_, _, _) =>
                    // Return the original error so users won't get confusing error message.
                    result
                  case _ => resultFromLastLine
                }

              case next :: nextTail =>
                interpretLines(head + "\n" + next :: nextTail, resultFromLastLine)
            }

          case InterpretError(_, _, _) => result

          case InterpretSuccess(e) =>
            val mergedRet = resultFromLastLine match {
              case InterpretSuccess(s) =>
                // Because of SparkMagic related specific logic, so we will only merge text/plain
                // result. For any magic related output, still follow the old way.
                if (s.values.contains(TEXT_PLAIN) && e.values.contains(TEXT_PLAIN)) {
                  val lastRet = s.values.getOrElse(TEXT_PLAIN, "").asInstanceOf[String]
                  val currRet = e.values.getOrElse(TEXT_PLAIN, "").asInstanceOf[String]
                  if (lastRet.nonEmpty && currRet.nonEmpty) {
                    InterpretSuccess(TEXT_PLAIN -> s"$lastRet$currRet")
                  } else if (lastRet.nonEmpty) {
                    InterpretSuccess(TEXT_PLAIN -> lastRet)
                  } else if (currRet.nonEmpty) {
                    InterpretSuccess(TEXT_PLAIN -> currRet)
                  } else {
                    result
                  }
                } else {
                  result
                }

              case _ => result
            }

            interpretLines(tail, mergedRet)
        }

    }
  }

  private def interpretLine(code: String): InterpretResponse = {
    code match {
      case MAGIC_REGEX(magic, rest) =>
        executeMagic(magic, rest)
      case _ =>
        this.interpret(code) match {
          case Results.Success => InterpretSuccess(TEXT_PLAIN -> getOutput)
          case Results.Incomplete => InterpretInComplete()
          case Results.Error => InterpretError("Error", getOutput)
        }
    }
  }

  private def executeMagic(magic: String, rest: String): InterpretResponse = {
    magic match {
      case "json" => executeJsonMagic(rest)
      case "table" => executeTableMagic(rest)
      case _ => InterpretError("UnknownMagic", s"Unknown magic command $magic")
    }
  }

  private def executeJsonMagic(name: String): InterpretResponse = {
    try {
      val value = valueOfTerm(name) match {
        case Some(obj: RDD[_]) => obj.asInstanceOf[RDD[_]].take(10)
        case Some(obj) => obj
        case None => return InterpretError("NameError", s"Value $name does not exist")
      }

      InterpretSuccess(APPLICATION_JSON -> toJson(value))
    } catch {
      case _: Throwable =>
        InterpretError("ValueError", "Failed to convert value into a JSON value")
    }
  }

  private def executeTableMagic(name: String): InterpretResponse = {
    val value = valueOfTerm(name) match {
      case Some(obj: RDD[_]) => obj.asInstanceOf[RDD[_]].take(10)
      case Some(obj) => obj
      case None => return InterpretError("NameError", s"Value $name does not exist")
    }

    extractTableFromJValue(toJson(value))
  }

  private class TypesDoNotMatch extends Exception

  private def extractTableFromJValue(value: JValue): InterpretResponse = {
    // Convert the value into JSON and map it to a table.
    val rows: List[JValue] = value match {
      case JArray(arr) => arr
      case _ => List(value)
    }

    try {
      val headers = scala.collection.mutable.Map[String, Map[String, String]]()

      val data = rows.map { case row =>
        val cols: List[JField] = row match {
          case JArray(arr: List[JValue]) =>
            arr.zipWithIndex.map { case (v, index) => JField(index.toString, v) }
          case JObject(obj) => obj.sortBy(_._1)
          case value: JValue => List(JField("0", value))
        }

        cols.map { case (k, v) =>
          val typeName = convertTableType(v)

          headers.get(k) match {
            case Some(header) =>
              if (header.get("type").get != typeName) {
                throw new TypesDoNotMatch
              }
            case None =>
              headers.put(
                k,
                Map(
                  "type" -> typeName,
                  "name" -> k))
          }

          v
        }
      }

      InterpretSuccess(
        APPLICATION_LIVY_TABLE_JSON -> (
          ("headers" -> headers.toSeq.sortBy(_._1).map(_._2)) ~ ("data" -> data)
        ))
    } catch {
      case _: TypesDoNotMatch =>
        InterpretError("TypeError", "table rows have different types")
    }
  }

  private def allSameType(values: Iterator[JValue]): Boolean = {
    if (values.hasNext) {
      val type_name = convertTableType(values.next())
      values.forall { case value => type_name.equals(convertTableType(value)) }
    } else {
      true
    }
  }

  private def convertTableType(value: JValue): String = {
    value match {
      case (JNothing | JNull) => "NULL_TYPE"
      case JBool(_) => "BOOLEAN_TYPE"
      case JString(_) => "STRING_TYPE"
      case JInt(_) => "BIGINT_TYPE"
      case JDouble(_) => "DOUBLE_TYPE"
      case JDecimal(_) => "DECIMAL_TYPE"
      case JArray(arr) =>
        if (allSameType(arr.iterator)) {
          "ARRAY_TYPE"
        } else {
          throw new TypesDoNotMatch
        }
      case JObject(obj) =>
        if (allSameType(obj.iterator.map(_._2))) {
          "MAP_TYPE"
        } else {
          throw new TypesDoNotMatch
        }
    }
  }

  private def valueOfTerm(name: String): Option[Any] = {
    // IMain#valueOfTerm will always return None, so use other way instead.
    Option(this.lastRequest.lineRep.call("$result"))
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
  private def withLockRequired[T](block: => T): T = Utils.withLockRequired(lock)(block)
}
