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

package org.apache.kyuubi.engine.dataagent.runtime;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ToolOutputStoreTest {

  private ToolOutputStore store;

  @BeforeEach
  public void setUp() throws IOException {
    store = ToolOutputStore.create();
  }

  @Test
  public void writeAndReadWindow() throws IOException {
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i <= 100; i++) sb.append("row").append(i).append('\n');
    Path p = store.write("sess1", "call1", sb.toString());
    assertTrue(Files.exists(p));

    String out = store.read("sess1", p.toString(), 10, 5);
    assertTrue(out.contains("lines 11-15 of"), out);
    assertTrue(out.contains("row11"), out);
    assertTrue(out.contains("row15"), out);
    assertFalse(out.contains("row16"), out);
    assertFalse(out.contains("row10"), out);
  }

  @Test
  public void grepReturnsMatchingLinesWithLineNumbers() throws IOException {
    String content = "apple\nbanana\ncherry\napple pie\ndate\n";
    Path p = store.write("sess1", "call1", content);

    String out = store.grep("sess1", p.toString(), "apple", 10);
    assertTrue(out.contains("1:apple"), out);
    assertTrue(out.contains("4:apple pie"), out);
    assertFalse(out.contains("banana"), out);
  }

  @Test
  public void grepRespectsMaxMatches() throws IOException {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 20; i++) sb.append("hit\n");
    Path p = store.write("sess1", "call1", sb.toString());

    String out = store.grep("sess1", p.toString(), "hit", 3);
    assertTrue(out.contains("[3 matches]"), out);
    assertTrue(out.contains("1:hit"), out);
    assertTrue(out.contains("3:hit"), out);
    assertFalse(out.contains("4:hit"), "should stop after 3 matches");
  }

  @Test
  public void grepInvalidRegexReturnsError() throws IOException {
    Path p = store.write("sess1", "call1", "x\n");
    String out = store.grep("sess1", p.toString(), "[", 10);
    assertTrue(out.startsWith("Error:"), out);
  }

  @Test
  public void readRejectsCrossSessionPath() throws IOException {
    Path victim = store.write("victim", "secret_call", "top secret\n");
    assertTrue(Files.exists(victim));

    String out = store.read("attacker", victim.toString(), 0, 10);
    assertTrue(out.startsWith("Error:"), out);
    assertFalse(out.contains("top secret"), out);
  }

  @Test
  public void grepRejectsCrossSessionPath() throws IOException {
    Path victim = store.write("victim", "secret_call", "api_key=xyz\n");
    String out = store.grep("attacker", victim.toString(), "api_key", 10);
    assertTrue(out.startsWith("Error:"), out);
    assertFalse(out.contains("xyz"), out);
  }

  @Test
  public void cleanupSessionRemovesSubtree() throws IOException {
    Path p1 = store.write("sessA", "call1", "a\n");
    Path p2 = store.write("sessA", "call2", "b\n");
    Path p3 = store.write("sessB", "call1", "c\n");
    assertTrue(Files.exists(p1));
    assertTrue(Files.exists(p2));
    assertTrue(Files.exists(p3));

    store.cleanupSession("sessA");

    assertFalse(Files.exists(p1));
    assertFalse(Files.exists(p2));
    assertTrue(Files.exists(p3), "other sessions untouched");
  }
}
