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

package org.apache.kyuubi.jdbc.hive.server;

import java.util.Arrays;

public final class CharTypes {
  protected static final char[] HC = "0123456789ABCDEF".toCharArray();
  protected static final byte[] HB;
  /**
   * Value used for lookup tables to indicate that matching characters do not need to be escaped.
   */
  public static final int ESCAPE_NONE = 0;

  /**
   * Value used for lookup tables to indicate that matching characters are to be escaped using
   * standard escaping; for JSON this means (for example) using "backslash - u" escape method.
   */
  public static final int ESCAPE_STANDARD = -1;

  static {
    int len = HC.length;
    HB = new byte[len];
    for (int i = 0; i < len; ++i) {
      HB[i] = (byte) HC[i];
    }
  }

  /**
   * Lookup table used for determining which input characters need special handling when contained
   * in text segment.
   */
  protected static final int[] sInputCodes;

  static {
    /* 96 would do for most cases (backslash is ASCII 94)
     * but if we want to do lookups by raw bytes it's better
     * to have full table
     */
    final int[] table = new int[256];
    // Control chars and non-space white space are not allowed unquoted
    for (int i = 0; i < 32; ++i) {
      table[i] = -1;
    }
    // And then string end and quote markers are special too
    table['"'] = 1;
    table['\\'] = 1;
    sInputCodes = table;
  }

  /** Additionally we can combine UTF-8 decoding info into similar data table. */
  protected static final int[] sInputCodesUTF8;

  static {
    final int[] table = new int[sInputCodes.length];
    System.arraycopy(sInputCodes, 0, table, 0, table.length);
    for (int c = 128; c < 256; ++c) {
      int code;

      // We'll add number of bytes needed for decoding
      if ((c & 0xE0) == 0xC0) { // 2 bytes (0x0080 - 0x07FF)
        code = 2;
      } else if ((c & 0xF0) == 0xE0) { // 3 bytes (0x0800 - 0xFFFF)
        code = 3;
      } else if ((c & 0xF8) == 0xF0) {
        // 4 bytes; double-char with surrogates and all...
        code = 4;
      } else {
        // And -1 seems like a good "universal" error marker...
        code = -1;
      }
      table[c] = code;
    }
    sInputCodesUTF8 = table;
  }

  /**
   * To support non-default (and -standard) unquoted field names mode, need to have alternate
   * checking. Basically this is list of 8-bit ASCII characters that are legal as part of Javascript
   * identifier
   */
  protected static final int[] sInputCodesJsNames;

  static {
    final int[] table = new int[256];
    // Default is "not a name char", mark ones that are
    Arrays.fill(table, -1);
    // Assume rules with JS same as Java (change if/as needed)
    for (int i = 33; i < 256; ++i) {
      if (Character.isJavaIdentifierPart((char) i)) {
        table[i] = 0;
      }
    }
    /* As per [JACKSON-267], '@', '#' and '*' are also to be accepted as well.
     * And '-' (for hyphenated names); and '+' for sake of symmetricity...
     */
    table['@'] = 0;
    table['#'] = 0;
    table['*'] = 0;
    table['-'] = 0;
    table['+'] = 0;
    sInputCodesJsNames = table;
  }

  /**
   * This table is similar to Latin-1, except that it marks all "high-bit" code as ok. They will be
   * validated at a later point, when decoding name
   */
  protected static final int[] sInputCodesUtf8JsNames;

  static {
    final int[] table = new int[256];
    // start with 8-bit JS names
    System.arraycopy(sInputCodesJsNames, 0, table, 0, table.length);
    Arrays.fill(table, 128, 128, 0);
    sInputCodesUtf8JsNames = table;
  }

  /**
   * Decoding table used to quickly determine characters that are relevant within comment content.
   */
  protected static final int[] sInputCodesComment;

  static {
    final int[] buf = new int[256];
    // but first: let's start with UTF-8 multi-byte markers:
    System.arraycopy(sInputCodesUTF8, 128, buf, 128, 128);

    // default (0) means "ok" (skip); -1 invalid, others marked by char itself
    Arrays.fill(buf, 0, 32, -1); // invalid white space
    buf['\t'] = 0; // tab is still fine
    buf['\n'] = '\n'; // lf/cr need to be observed, ends cpp comment
    buf['\r'] = '\r';
    buf['*'] = '*'; // end marker for c-style comments
    sInputCodesComment = buf;
  }

  /**
   * Decoding table used for skipping white space and comments.
   *
   * @since 2.3
   */
  protected static final int[] sInputCodesWS;

  static {
    // but first: let's start with UTF-8 multi-byte markers:
    final int[] buf = new int[256];
    System.arraycopy(sInputCodesUTF8, 128, buf, 128, 128);

    // default (0) means "not whitespace" (end); 1 "whitespace", -1 invalid,
    // 2-4 UTF-8 multi-bytes, others marked by char itself
    //
    Arrays.fill(buf, 0, 32, -1); // invalid white space
    buf[' '] = 1;
    buf['\t'] = 1;
    buf['\n'] = '\n'; // lf/cr need to be observed, ends cpp comment
    buf['\r'] = '\r';
    buf['/'] = '/'; // start marker for c/cpp comments
    buf['#'] = '#'; // start marker for YAML comments
    sInputCodesWS = buf;
  }

  /**
   * Lookup table used for determining which output characters in 7-bit ASCII range need to be
   * quoted.
   */
  protected static final int[] sOutputEscapes128;

  static {
    int[] table = new int[128];
    // Control chars need generic escape sequence
    for (int i = 0; i < 32; ++i) {
      // 04-Mar-2011, tatu: Used to use "-(i + 1)", replaced with constant
      table[i] = ESCAPE_STANDARD;
    }
    // Others (and some within that range too) have explicit shorter sequences
    table['"'] = '"';
    table['\\'] = '\\';
    // Escaping of slash is optional, so let's not add it
    table[0x08] = 'b';
    table[0x09] = 't';
    table[0x0C] = 'f';
    table[0x0A] = 'n';
    table[0x0D] = 'r';
    sOutputEscapes128 = table;
  }

  /**
   * Lookup table for the first 256 Unicode characters (ASCII / UTF-8) range. For actual hex digits,
   * contains corresponding value; for others -1.
   *
   * <p>NOTE: before 2.10.1, was of size 128, extended for simpler handling
   */
  protected static final int[] sHexValues = new int[256];

  static {
    Arrays.fill(sHexValues, -1);
    for (int i = 0; i < 10; ++i) {
      sHexValues['0' + i] = i;
    }
    for (int i = 0; i < 6; ++i) {
      sHexValues['a' + i] = 10 + i;
      sHexValues['A' + i] = 10 + i;
    }
  }

  public static int[] getInputCodeLatin1JsNames() {
    return sInputCodesJsNames;
  }

  public static int[] getInputCodeUtf8JsNames() {
    return sInputCodesUtf8JsNames;
  }
}
