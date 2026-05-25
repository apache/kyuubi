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

package org.apache.hive.beeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.hive.beeline.common.HiveTestUtils;
import org.apache.kyuubi.util.JavaUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unit test for Beeline arg parser. */
public class TestBeelineArgParsing {
  private static final Logger LOG = LoggerFactory.getLogger(TestBeelineArgParsing.class.getName());

  private static final String dummyDriverClazzName = "DummyDriver";

  public class TestBeeline extends BeeLine {

    String connectArgs = null;
    List<String> properties = new ArrayList<String>();
    List<String> queries = new ArrayList<String>();

    @Override
    boolean dispatch(String command) {
      String connectCommand = "!connect";
      String propertyCommand = "!properties";
      if (command.startsWith(connectCommand)) {
        this.connectArgs = command.substring(connectCommand.length() + 1, command.length());
      } else if (command.startsWith(propertyCommand)) {
        this.properties.add(command.substring(propertyCommand.length() + 1, command.length()));
      } else {
        this.queries.add(command);
      }
      return true;
    }

    public boolean addlocaldrivername(String driverName) {
      String line = "addlocaldrivername " + driverName;
      return getCommands().addlocaldrivername(line);
    }

    public boolean addLocalJar(String url) {
      String line = "addlocaldriverjar " + url;
      return getCommands().addlocaldriverjar(line);
    }
  }

  public static Stream<Arguments> data() throws IOException, InterruptedException {
    // generate the dummy driver by using txt file
    String u = HiveTestUtils.getFileFromClasspath("DummyDriver.txt");
    Map<File, String> extraContent = new HashMap<>();
    extraContent.put(new File("META-INF/services/java.sql.Driver"), dummyDriverClazzName);
    File jarFile = HiveTestUtils.genLocalJarForTest(u, dummyDriverClazzName, extraContent);
    String pathToDummyDriver = jarFile.getAbsolutePath();
    String kyuubiHome =
        JavaUtils.getCodeSourceLocation(TestBeelineArgParsing.class)
            .split("kyuubi-hive-beeline")[0];

    Path jarsDir = Paths.get(kyuubiHome).resolve("kyuubi-hive-beeline").resolve("target");

    String postgresqlJdbcDriverPath =
        Files.list(jarsDir)
            .filter(p -> p.getFileName().toString().contains("postgresql"))
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalStateException("Can not find PostgreSQL JDBC driver in " + jarsDir))
            .toAbsolutePath()
            .toString();

    return Stream.of(
        Arguments.of(
            "jdbc:postgresql://host:5432/testdb",
            "org.postgresql.Driver",
            postgresqlJdbcDriverPath,
            true),
        Arguments.of(
            "jdbc:dummy://host:5432/testdb", dummyDriverClazzName, pathToDummyDriver, false));
  }

  @Test
  public void testSimpleArgs() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] =
        new String[] {
          "-u", "url", "-n", "name", "-p", "password", "-d", "driver", "-a", "authType"
        };
    assertEquals(0, bl.initArgs(args));
    assertTrue(bl.connectArgs.equals("url name password driver"));
    assertTrue(bl.getOpts().getAuthType().equals("authType"));
  }

  @Test
  public void testPasswordFileArgs() throws Exception {
    TestBeeline bl = new TestBeeline();
    File passFile = new File("file.password");
    passFile.deleteOnExit();
    FileOutputStream passFileOut = new FileOutputStream(passFile);
    passFileOut.write("mypass\n".getBytes());
    passFileOut.close();
    String args[] =
        new String[] {
          "-u",
          "url",
          "-n",
          "name",
          "-w",
          "file.password",
          "-p",
          "not-taken-if-w-is-present",
          "-d",
          "driver",
          "-a",
          "authType"
        };
    bl.initArgs(args);
    System.out.println(bl.connectArgs);
    // Password file contents are trimmed of trailing whitespaces and newlines
    assertTrue(bl.connectArgs.equals("url name mypass driver"));
    assertTrue(bl.getOpts().getAuthType().equals("authType"));
    passFile.delete();
  }

  /** The first flag is taken by the parser. */
  @Test
  public void testDuplicateArgs() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] =
        new String[] {"-u", "url", "-u", "url2", "-n", "name", "-p", "password", "-d", "driver"};
    assertEquals(0, bl.initArgs(args));
    assertTrue(bl.connectArgs.equals("url name password driver"));
  }

  @Test
  public void testQueryScripts() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] =
        new String[] {
          "-u",
          "url",
          "-n",
          "name",
          "-p",
          "password",
          "-d",
          "driver",
          "-e",
          "select1",
          "-e",
          "select2"
        };
    assertEquals(0, bl.initArgs(args));
    assertTrue(bl.connectArgs.equals("url name password driver"));
    assertTrue(bl.queries.contains("select1"));
    assertTrue(bl.queries.contains("select2"));
  }

  /** Test setting hive conf and hive vars with --hiveconf, --hivevar and --conf */
  @Test
  public void testHiveConfAndVars() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] =
        new String[] {
          "-u",
          "url",
          "-n",
          "name",
          "-p",
          "password",
          "-d",
          "driver",
          "--hiveconf",
          "a=avalue",
          "--hiveconf",
          "b=bvalue",
          "--hivevar",
          "c=cvalue",
          "--hivevar",
          "d=dvalue",
          "--conf",
          "e=evalue",
          "--conf",
          "f=fvalue"
        };
    assertEquals(0, bl.initArgs(args));
    assertTrue(bl.connectArgs.equals("url name password driver"));
    assertTrue(bl.getOpts().getHiveConfVariables().get("a").equals("avalue"));
    assertTrue(bl.getOpts().getHiveConfVariables().get("b").equals("bvalue"));
    assertTrue(bl.getOpts().getHiveVariables().get("c").equals("cvalue"));
    assertTrue(bl.getOpts().getHiveVariables().get("d").equals("dvalue"));
    assertTrue(bl.getOpts().getHiveConfVariables().get("e").equals("evalue"));
    assertTrue(bl.getOpts().getHiveConfVariables().get("f").equals("fvalue"));
  }

  @Test
  public void testBeelineOpts() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] =
        new String[] {
          "-u",
          "url",
          "-n",
          "name",
          "-p",
          "password",
          "-d",
          "driver",
          "--autoCommit=true",
          "--verbose",
          "--truncateTable"
        };
    assertEquals(0, bl.initArgs(args));
    assertTrue(bl.connectArgs.equals("url name password driver"));
    assertTrue(bl.getOpts().getAutoCommit());
    assertTrue(bl.getOpts().getVerbose());
    assertTrue(bl.getOpts().getTruncateTable());
  }

  @Test
  public void testBeelineAutoCommit() throws Exception {
    TestBeeline bl = new TestBeeline();
    String[] args = {};
    bl.initArgs(args);
    assertTrue(bl.getOpts().getAutoCommit());

    args = new String[] {"--autoCommit=false"};
    bl.initArgs(args);
    assertFalse(bl.getOpts().getAutoCommit());

    args = new String[] {"--autoCommit=true"};
    bl.initArgs(args);
    assertTrue(bl.getOpts().getAutoCommit());
    bl.close();
  }

  @Test
  public void testBeelineShowDbInPromptOptsDefault() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] = new String[] {"-u", "url"};
    assertEquals(0, bl.initArgs(args));
    assertFalse(bl.getOpts().getShowDbInPrompt());
    assertEquals("", bl.getFormattedDb());
  }

  @Test
  public void testBeelineShowDbInPromptOptsTrue() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] = new String[] {"-u", "url", "--showDbInPrompt=true"};
    assertEquals(0, bl.initArgs(args));
    assertTrue(bl.getOpts().getShowDbInPrompt());
    assertEquals(" (default)", bl.getFormattedDb());
  }

  /** Test setting script file with -f option. */
  @Test
  public void testScriptFile() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] =
        new String[] {
          "-u", "url", "-n", "name", "-p", "password", "-d", "driver", "-f", "myscript"
        };
    assertEquals(0, bl.initArgs(args));
    assertTrue(bl.connectArgs.equals("url name password driver"));
    assertTrue(bl.getOpts().getScriptFile().equals("myscript"));
  }

  /** Test beeline with -f and -e simultaneously */
  @Test
  public void testCommandAndFileSimultaneously() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] = new String[] {"-e", "myselect", "-f", "myscript"};
    assertEquals(1, bl.initArgs(args));
  }

  /** Test beeline with multiple initfiles in -i. */
  @Test
  public void testMultipleInitFiles() {
    TestBeeline bl = new TestBeeline();
    String[] args = new String[] {"-i", "/url/to/file1", "-i", "/url/to/file2"};
    assertEquals(0, bl.initArgs(args));
    String[] files = bl.getOpts().getInitFiles();
    assertEquals("/url/to/file1", files[0]);
    assertEquals("/url/to/file2", files[1]);
  }

  /** Displays the usage. */
  @Test
  public void testHelp() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] = new String[] {"--help"};
    assertEquals(0, bl.initArgs(args));
    assertEquals(true, bl.getOpts().isHelpAsked());
  }

  /** Displays the usage. */
  @Test
  public void testUnmatchedArgs() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] = new String[] {"-u", "url", "-n"};
    assertEquals(-1, bl.initArgs(args));
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("data")
  public void testAddLocalJar(
      String connectionString,
      String driverClazzName,
      String driverJarFileName,
      boolean defaultSupported)
      throws Exception {
    TestBeeline bl = new TestBeeline();
    assertNull(bl.findLocalDriver(connectionString));

    LOG.info("Add " + driverJarFileName + " for the driver class " + driverClazzName);

    bl.addLocalJar(driverJarFileName);
    bl.addlocaldrivername(driverClazzName);
    assertEquals(driverClazzName, bl.findLocalDriver(connectionString).getClass().getName());
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("data")
  public void testAddLocalJarWithoutAddDriverClazz(
      String connectionString,
      String driverClazzName,
      String driverJarFileName,
      boolean defaultSupported)
      throws Exception {
    TestBeeline bl = new TestBeeline();

    LOG.info("Add " + driverJarFileName + " for the driver class " + driverClazzName);
    assertTrue(new File(driverJarFileName).exists(), "expected to exists: " + driverJarFileName);
    bl.addLocalJar(driverJarFileName);
    if (!defaultSupported) {
      assertNull(bl.findLocalDriver(connectionString));
    } else {
      // no need to add for the default supported local jar driver
      assertNotNull(bl.findLocalDriver(connectionString));
      assertEquals(driverClazzName, bl.findLocalDriver(connectionString).getClass().getName());
    }
  }

  @Test
  public void testBeelinePasswordMask() throws Exception {
    TestBeeline bl = new TestBeeline();
    File errFile = File.createTempFile("test", "tmp");
    bl.setErrorStream(new PrintStream(new FileOutputStream(errFile)));
    String args[] =
        new String[] {
          "-u",
          "url",
          "-n",
          "name",
          "-p",
          "password",
          "-d",
          "driver",
          "--autoCommit=true",
          "--verbose",
          "--truncateTable"
        };
    bl.initArgs(args);
    bl.close();
    String errContents = new String(Files.readAllBytes(Paths.get(errFile.toString())));
    assertTrue(errContents.contains(BeeLine.PASSWD_MASK));
  }

  /** Test property file parameter option. */
  @Test
  public void testPropertyFile() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] = new String[] {"--property-file", "props"};
    assertEquals(0, bl.initArgs(args));
    assertTrue(bl.properties.get(0).equals("props"));
    bl.close();
  }

  /** Test maxHistoryRows parameter option. */
  @Test
  public void testMaxHistoryRows() throws Exception {
    TestBeeline bl = new TestBeeline();
    String args[] = new String[] {"--maxHistoryRows=100"};
    assertEquals(0, bl.initArgs(args));
    assertTrue(bl.getOpts().getMaxHistoryRows() == 100);
    bl.close();
  }
}
