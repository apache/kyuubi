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

package org.apache.hive.beeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import org.apache.kyuubi.util.reflect.DynFields;
import org.junit.Test;

public class KyuubiBeeLineTest {
  @Test
  public void testKyuubiBeelineWithoutArgs() {
    KyuubiBeeLine kyuubiBeeLine = new KyuubiBeeLine();
    int result = kyuubiBeeLine.initArgs(new String[0]);
    assertEquals(0, result);
  }

  @Test
  public void testKyuubiBeelineExitCodeWithoutConnection() {
    KyuubiBeeLine kyuubiBeeLine = new KyuubiBeeLine();
    String scriptFile = getClass().getClassLoader().getResource("test.sql").getFile();

    String[] args1 = {"-u", "badUrl", "-e", "show tables"};
    int result1 = kyuubiBeeLine.initArgs(args1);
    assertEquals(1, result1);

    String[] args2 = {"-u", "badUrl", "-f", scriptFile};
    int result2 = kyuubiBeeLine.initArgs(args2);
    assertEquals(1, result2);

    String[] args3 = {"-u", "badUrl", "-i", scriptFile};
    int result3 = kyuubiBeeLine.initArgs(args3);
    assertEquals(1, result3);
  }

  @Test
  public void testKyuubiBeeLineCmdUsage() {
    BufferPrintStream printStream = new BufferPrintStream();

    KyuubiBeeLine kyuubiBeeLine = new KyuubiBeeLine();
    DynFields.builder()
        .hiddenImpl(BeeLine.class, "outputStream")
        .build(kyuubiBeeLine)
        .set(printStream);
    String[] args1 = {"-h"};
    kyuubiBeeLine.initArgs(args1);
    String output = printStream.getOutput();
    assert output.contains("--python-mode                   Execute python code/script.");
  }

  @Test
  public void testKyuubiBeeLinePythonMode() {
    KyuubiBeeLine kyuubiBeeLine = new KyuubiBeeLine();
    String[] args1 = {"-u", "badUrl", "--python-mode"};
    kyuubiBeeLine.initArgs(args1);
    assertTrue(kyuubiBeeLine.isPythonMode());
    kyuubiBeeLine.setPythonMode(false);

    String[] args2 = {"--python-mode", "-f", "test.sql"};
    kyuubiBeeLine.initArgs(args2);
    assertTrue(kyuubiBeeLine.isPythonMode());
    assert kyuubiBeeLine.getOpts().getScriptFile().equals("test.sql");
    kyuubiBeeLine.setPythonMode(false);

    String[] args3 = {"-u", "badUrl"};
    kyuubiBeeLine.initArgs(args3);
    assertTrue(!kyuubiBeeLine.isPythonMode());
    kyuubiBeeLine.setPythonMode(false);
  }

  @Test
  public void testKyuubiBeelineComment() {
    KyuubiBeeLine kyuubiBeeLine = new KyuubiBeeLine();
    int result = kyuubiBeeLine.initArgsFromCliVars(new String[] {"-e", "--comment show database;"});
    assertEquals(0, result);
    result = kyuubiBeeLine.initArgsFromCliVars(new String[] {"-e", "--comment\n show database;"});
    assertEquals(1, result);
    result =
        kyuubiBeeLine.initArgsFromCliVars(
            new String[] {"-e", "--comment line 1 \n    --comment line 2 \n show database;"});
    assertEquals(1, result);
  }

  static class BufferPrintStream extends PrintStream {
    public StringBuilder stringBuilder = new StringBuilder();

    static OutputStream noOpOutputStream =
        new OutputStream() {
          @Override
          public void write(int b) throws IOException {
            // do nothing
          }
        };

    public BufferPrintStream() {
      super(noOpOutputStream);
    }

    public BufferPrintStream(OutputStream outputStream) {
      super(noOpOutputStream);
    }

    @Override
    public void println(String x) {
      stringBuilder.append(x).append("\n");
    }

    @Override
    public void print(String x) {
      stringBuilder.append(x);
    }

    public String getOutput() {
      return stringBuilder.toString();
    }
  }
}
