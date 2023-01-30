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

package org.apache.kyuubi

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}
import java.sql.ResultSet

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.jakewharton.fliptables.FlipTable
import com.vladsch.flexmark.formatter.Formatter
import com.vladsch.flexmark.parser.{Parser, ParserEmulationProfile, PegdownExtensions}
import com.vladsch.flexmark.profile.pegdown.PegdownOptionsAdapter
import com.vladsch.flexmark.util.data.{MutableDataHolder, MutableDataSet}
import com.vladsch.flexmark.util.sequence.SequenceUtils
import org.scalatest.Assertions.{assertResult, withClue}

object TestUtils {

  private def formatMarkdown(lines: ArrayBuffer[String]): ArrayBuffer[String] = {
    def createParserOptions(emulationProfile: ParserEmulationProfile): MutableDataHolder = {
      PegdownOptionsAdapter.flexmarkOptions(PegdownExtensions.ALL).toMutable
        .set(Parser.PARSER_EMULATION_PROFILE, emulationProfile)
    }

    def createFormatterOptions(
        parserOptions: MutableDataHolder,
        emulationProfile: ParserEmulationProfile): MutableDataSet = {
      new MutableDataSet()
        .set(Parser.EXTENSIONS, Parser.EXTENSIONS.get(parserOptions))
        .set(Formatter.FORMATTER_EMULATION_PROFILE, emulationProfile)
    }

    val emulationProfile = ParserEmulationProfile.valueOf("COMMONMARK")
    val parserOptions = createParserOptions(emulationProfile)
    val formatterOptions = createFormatterOptions(parserOptions, emulationProfile)
    val parser = Parser.builder(parserOptions).build
    val renderer = Formatter.builder(formatterOptions).build
    val document = parser.parse(lines.mkString(SequenceUtils.EOL))
    val formattedLines = new ArrayBuffer[String]
    val formattedLinesAppendable = new Appendable {
      override def append(csq: CharSequence): Appendable = {
        if (csq.length() > 0) {
          formattedLines.append(csq.toString)
        }
        this
      }

      override def append(csq: CharSequence, start: Int, end: Int): Appendable = {
        append(csq.toString.substring(start, end))
      }

      override def append(c: Char): Appendable = {
        append(c.toString)
      }
    }
    renderer.render(document, formattedLinesAppendable)
    // trim the ending EOL appended by renderer for each line
    formattedLines.map(str =>
      if (str.nonEmpty && str.endsWith(SequenceUtils.EOL)) {
        str.substring(0, str.length - 1)
      } else {
        str
      })
  }

  def verifyOutput(
      markdown: Path,
      newOutput: ArrayBuffer[String],
      agent: String,
      module: String): Unit = {
    if (System.getenv("KYUUBI_UPDATE") == "1") {
      val formatted = formatMarkdown(newOutput)
      Files.write(
        markdown,
        formatted.asJava,
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING)
    } else {
      val linesInFile = Files.readAllLines(markdown, StandardCharsets.UTF_8)
      val formatted = formatMarkdown(newOutput)
      linesInFile.asScala.zipWithIndex.zip(formatted).foreach { case ((str1, index), str2) =>
        withClue(s"$markdown out of date, as line ${index + 1} is not expected." +
          " Please update doc with KYUUBI_UPDATE=1 build/mvn clean test" +
          s" -pl $module -am -Pflink-provided,spark-provided,hive-provided" +
          s" -DwildcardSuites=$agent") {
          assertResult(str2)(str1)
        }
      }
    }
  }

  def displayResultSet(resultSet: ResultSet): Unit = {
    if (resultSet == null) throw new NullPointerException("resultSet == null")
    val resultSetMetaData = resultSet.getMetaData
    val columnCount: Int = resultSetMetaData.getColumnCount
    val headers = (1 to columnCount).map(resultSetMetaData.getColumnName).toArray
    val data = ArrayBuffer.newBuilder[Array[String]]
    while (resultSet.next) {
      data += (1 to columnCount).map(resultSet.getString).toArray
    }
    // scalastyle:off println
    println(FlipTable.of(headers, data.result().toArray))
    // scalastyle:on println
  }
}
