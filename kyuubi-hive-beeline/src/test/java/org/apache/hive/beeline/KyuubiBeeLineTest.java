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
    assert output.contains("--python-mode=[true/false]      Execute python code/script.");
    printStream.reset();
  }

  @Test
  public void testKyuubiBeeLinePythonMode() {
    KyuubiBeeLine kyuubiBeeLine = new KyuubiBeeLine();
    String[] args = {"-u", "badUrl", "--python-mode=true"};
    kyuubiBeeLine.initArgs(args);
    assertEquals(kyuubiBeeLine.isPythonMode(), true);
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

    public void reset() {
      stringBuilder = new StringBuilder();
    }
  }
}
