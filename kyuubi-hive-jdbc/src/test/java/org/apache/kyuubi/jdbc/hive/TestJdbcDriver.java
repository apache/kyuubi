/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.jdbc.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestJdbcDriver {
  private static File file = null;

  static Stream<Arguments> data() {
    return Stream.of(
        // Here are some positive cases which can be executed as below :
        Arguments.of("show databases;show tables;", "show databases,show tables"),
        Arguments.of(" show\n\r  tables;", "show tables"),
        Arguments.of("show databases; show\ntables;", "show databases,show tables"),
        Arguments.of("show    tables;", "show    tables"),
        Arguments.of("show tables ;", "show tables"),
        // Here are some negative cases as below :
        Arguments.of("show tables", ","),
        Arguments.of("show tables show tables;", "show tables show tables"),
        Arguments.of("show tab les;", "show tab les"),
        Arguments.of("#show tables; show\n tables;", "tables"),
        Arguments.of("show tab les;show tables;", "show tab les,show tables"));
  }

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    file = new File(System.getProperty("user.dir") + File.separator + "Init.sql");
    if (!file.exists()) {
      Files.createFile(file.toPath());
    }
  }

  @AfterAll
  public static void cleanUpAfterClass() throws Exception {
    if (file != null) {
      Files.deleteIfExists(file.toPath());
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testParseInitFile(String input, String expected) throws IOException {
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
      bw.write(input);
      bw.flush();
      assertEquals(
          Arrays.asList(expected.split(",")), KyuubiConnection.parseInitFile(file.toString()));
    }
  }
}
