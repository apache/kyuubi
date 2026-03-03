/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.beeline.common.util;

import com.google.common.base.Splitter;
import java.util.Iterator;

/**
 * HiveStringUtils General string utils
 *
 * <p>Originally copied from o.a.hadoop.util.StringUtils
 */
public class HiveStringUtils {

  /**
   * Strip comments from a sql statement, tracking when the statement contains a string literal.
   *
   * @param statement the input string
   * @return a stripped statement
   */
  public static String removeComments(String statement) {
    if (statement == null) {
      return null;
    }
    Iterator<String> iterator = Splitter.on("\n").omitEmptyStrings().split(statement).iterator();
    int[] startQuote = {-1};
    StringBuilder ret = new StringBuilder(statement.length());
    while (iterator.hasNext()) {
      String lineWithComments = iterator.next();
      String lineNoComments = removeComments(lineWithComments, startQuote);
      ret.append(lineNoComments);
      if (iterator.hasNext() && !lineNoComments.isEmpty()) {
        ret.append("\n");
      }
    }
    return ret.toString();
  }

  /**
   * Remove comments from the current line of a query. Avoid removing comment-like strings inside
   * quotes.
   *
   * @param line a line of sql text
   * @param startQuote The value -1 indicates that line does not begin inside a string literal.
   *     Other values indicate that line does begin inside a string literal and the value passed is
   *     the delimiter character. The array type is used to pass int type as input/output parameter.
   * @return the line with comments removed.
   */
  public static String removeComments(String line, int[] startQuote) {
    if (line == null || line.isEmpty()) {
      return line;
    }
    if (startQuote[0] == -1 && isComment(line)) {
      return ""; // assume # can only be used at the beginning of line.
    }
    StringBuilder builder = new StringBuilder();
    for (int index = 0; index < line.length(); ) {
      if (startQuote[0] == -1
          && index < line.length() - 1
          && line.charAt(index) == '-'
          && line.charAt(index + 1) == '-') {
        // Jump to the end of current line. When a multiple line query is executed with -e
        // parameter,
        // it is passed in as one line string separated with '\n'
        for (; index < line.length() && line.charAt(index) != '\n'; ++index)
          ;
        continue;
      }

      char letter = line.charAt(index);
      if (startQuote[0] == letter && (index == 0 || line.charAt(index - 1) != '\\')) {
        startQuote[0] = -1; // Turn escape off.
      } else if (startQuote[0] == -1
          && (letter == '\'' || letter == '"')
          && (index == 0 || line.charAt(index - 1) != '\\')) {
        startQuote[0] = letter; // Turn escape on.
      }

      builder.append(letter);
      index++;
    }

    return builder.toString().trim();
  }

  /**
   * Test whether a line is a comment.
   *
   * @param line the line to be tested
   * @return true if a comment
   */
  private static boolean isComment(String line) {
    // SQL92 comment prefix is "--"
    // beeline also supports shell-style "#" prefix
    String lineTrimmed = line.trim();
    return lineTrimmed.startsWith("#") || lineTrimmed.startsWith("--");
  }
}
