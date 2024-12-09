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
package org.apache.kyuubi.util

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.Traversable
import scala.io.Source
import scala.reflect.ClassTag
import scala.util.matching.Regex

import org.scalactic.Prettifier
import org.scalactic.source.Position
import org.scalatest.Assertions._

import org.apache.kyuubi.util.GoldenFileUtils.getLicenceContent

object AssertionUtils {

  def assertEqualsIgnoreCase(expected: AnyRef)(actual: AnyRef)(
      implicit pos: Position): Unit = {
    val isEqualsIgnoreCase = (Option(expected), Option(actual)) match {
      case (Some(expectedStr: String), Some(actualStr: String)) =>
        expectedStr.equalsIgnoreCase(actualStr)
      case (Some(Some(expectedStr: String)), Some(Some(actualStr: String))) =>
        expectedStr.equalsIgnoreCase(actualStr)
      case (None, None) => true
      case _ => false
    }
    if (!isEqualsIgnoreCase) {
      fail(s"Expected equaling to '$expected' ignoring case, but got '$actual'")(pos)
    }
  }

  def assertStartsWithIgnoreCase(expectedPrefix: String)(actual: String)(implicit
      pos: Position): Unit = {
    if (!actual.toLowerCase(Locale.ROOT).startsWith(expectedPrefix.toLowerCase(Locale.ROOT))) {
      fail(s"Expected starting with '$expectedPrefix' ignoring case, but got [$actual]")(pos)
    }
  }

  def assertExistsIgnoreCase(expected: String)(actual: Iterable[String])(implicit
      pos: Position): Unit = {
    if (!actual.exists(_.equalsIgnoreCase(expected))) {
      fail(s"Expected containing '$expected' ignoring case, but got [$actual]")(pos)
    }
  }

  /**
   * Assert the file content is equal to the expected lines.
   * If not, throws assertion error with the given regeneration hint.
   * @param expectedLines expected lines
   * @param path source file path
   * @param regenScript regeneration script
   * @param splitFirstExpectedLine whether to split the first expected line
   *                               into multiple lines by EOL
   */
  def assertFileContent(
      path: Path,
      expectedLines: Traversable[String],
      regenScript: String,
      splitFirstExpectedLine: Boolean = false)(implicit
      prettifier: Prettifier,
      pos: Position): Unit = {
    val fileSource = Source.fromFile(path.toUri, StandardCharsets.UTF_8.name())
    try {
      def expectedLinesIter = if (splitFirstExpectedLine) {
        Source.fromString(expectedLines.head).getLines()
      } else {
        expectedLines.toIterator
      }
      val fileLinesIter = fileSource.getLines()
      val regenerationHint = s"The file ($path) is out of date. " + {
        if (regenScript != null && regenScript.nonEmpty) {
          s" Please regenerate it by running `${regenScript.stripMargin}`. "
        } else ""
      }
      var fileLineCount = 0
      fileLinesIter.zipWithIndex.zip(expectedLinesIter)
        .foreach { case ((lineInFile, lineIndex), expectedLine) =>
          val lineNum = lineIndex + 1
          withClue(s"Line $lineNum is not expected. $regenerationHint") {
            assertResult(expectedLine)(lineInFile)(prettifier, pos)
          }
          fileLineCount = Math.max(lineNum, fileLineCount)
        }
      withClue(s"Line number is not expected. $regenerationHint") {
        assertResult(expectedLinesIter.size)(fileLineCount)(prettifier, pos)
      }
    } finally {
      fileSource.close()
    }
  }

  def assertFileContentSorted(
      filePath: Path,
      headerSkipPrefix: String = "#",
      licenceHeader: Iterable[String] = getLicenceContent(),
      distinct: Boolean = true): Unit = {
    val sortedLines = Files.readAllLines(filePath).asScala
      .dropWhile(line => line.trim == "" || line.trim.startsWith(headerSkipPrefix))
      .map(_.trim).filter(_.nonEmpty)
      .sorted
    val expectedSortedLines = if (distinct) {
      sortedLines.distinct
    } else {
      sortedLines
    }
    val expectedLines = licenceHeader ++ Seq("") ++ expectedSortedLines
    assertFileContent(filePath, expectedLines, s"Check SPI provider file sorted $filePath")
  }

  /**
   * Assert the iterable contains all the expected elements
   */
  def assertContains(actual: TraversableOnce[AnyRef], expected: AnyRef*)(implicit
      prettifier: Prettifier,
      pos: Position): Unit =
    withClue(s", expected containing [${expected.mkString(", ")}]") {
      val actualSeq = actual.toSeq
      expected.foreach { elem => assert(actualSeq.contains(elem))(prettifier, pos) }
    }

  /**
   * Asserts the string matches the regex
   */
  def assertMatches(actual: String, regex: Regex)(implicit
      prettifier: Prettifier,
      pos: Position): Unit =
    withClue(s"'$actual' expected matching the regex '$regex'") {
      assert(regex.findFirstMatchIn(actual).isDefined)(prettifier, pos)
    }

  /**
   * Asserts the string does not match the regex
   */
  def assertNotMatches(actual: String, regex: Regex)(implicit
      prettifier: Prettifier,
      pos: Position): Unit =
    withClue(s"'$actual' expected not matching the regex '$regex'") {
      assert(regex.findFirstMatchIn(actual).isEmpty)(prettifier, pos)
    }

  /**
   * Asserts that the given function throws an exception of the given type
   * and with the exception message equals to expected string
   */
  def interceptEquals[T <: Exception](f: => Any)(expected: String)(implicit
      classTag: ClassTag[T],
      pos: Position): Unit = {
    assert(expected != null)
    val exception = intercept[T](f)(classTag, pos)
    assertResult(expected)(exception.getMessage)
  }

  /**
   * Asserts that the given function throws an exception of the given type
   * and with the exception message contains expected string
   */
  def interceptContains[T <: Exception](f: => Any)(contained: String)(implicit
      classTag: ClassTag[T],
      pos: Position): Unit = {
    assert(contained != null)
    val exception = intercept[T](f)(classTag, pos)
    assert(exception.getMessage.contains(contained))
  }

  /**
   * Asserts that the given function throws an exception of the given type
   * and with the exception message ends with expected string
   */
  def interceptEndsWith[T <: Exception](f: => Any)(end: String)(implicit
      classTag: ClassTag[T],
      pos: Position): Unit = {
    assert(end != null)
    val exception = intercept[T](f)(classTag, pos)
    assert(exception.getMessage.endsWith(end))
  }

  /**
   * Asserts that the given function throws an exception of the given type T
   * with a cause of type Q and with the cause message of the exception equals to expected string
   */
  def interceptCauseContains[T <: Exception, Q <: Throwable](f: => Any)(contained: String)(
      implicit
      classTag: ClassTag[T],
      pos: Position): Unit = {
    assert(contained != null)
    val exception = intercept[T](f)(classTag, pos)
    assert(exception.getCause.asInstanceOf[Q].getMessage.contains(contained))
  }
}
