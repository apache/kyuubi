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
import java.util.List;
import jline.console.ConsoleReader;
import org.junit.Test;
import org.mockito.Mockito;

public class KyuubiCommandsTest {
  @Test
  public void testParsePythonSnippets() throws IOException {
    ConsoleReader reader = Mockito.mock(ConsoleReader.class);
    String pythonSnippets = "for i in [1, 2, 3]:\n" + "    print(i)\n";
    Mockito.when(reader.readLine()).thenReturn(pythonSnippets);

    KyuubiBeeLine beeline = new KyuubiBeeLine();
    beeline.setPythonMode(true);
    beeline.setConsoleReader(reader);
    KyuubiCommands commands = new KyuubiCommands(beeline);
    String line = commands.handleMultiLineCmd(pythonSnippets);

    List<String> cmdList = commands.getCmdList(line, false);
    assertEquals(cmdList.size(), 1);
    assertEquals(cmdList.get(0), pythonSnippets);
  }

  @Test
  public void testHandleMultiLineCmd() throws IOException {
    ConsoleReader reader = Mockito.mock(ConsoleReader.class);
    String snippets = "select 1;--comments1\nselect 2;--comments2";
    Mockito.when(reader.readLine()).thenReturn(snippets);

    KyuubiBeeLine beeline = new KyuubiBeeLine();
    beeline.setConsoleReader(reader);
    beeline.setPythonMode(false);
    KyuubiCommands commands = new KyuubiCommands(beeline);
    String line = commands.handleMultiLineCmd(snippets);
    List<String> cmdList = commands.getCmdList(line, false);
    assertEquals(cmdList.size(), 2);
    assertEquals(cmdList.get(0), "select 1");
    assertEquals(cmdList.get(1), "\nselect 2");

    // see HIVE-15820: comment at the head of beeline -e
    snippets = "--comments1\nselect 2;--comments2";
    Mockito.when(reader.readLine()).thenReturn(snippets);
    line = commands.handleMultiLineCmd(snippets);
    cmdList = commands.getCmdList(line, false);
    assertEquals(cmdList.size(), 1);
    assertEquals(cmdList.get(0), "select 2");
  }
}
