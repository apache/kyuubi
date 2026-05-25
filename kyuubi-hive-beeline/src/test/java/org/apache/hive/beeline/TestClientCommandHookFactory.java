/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.beeline;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestClientCommandHookFactory {
  public BeeLine setupMockData(boolean isBeeLine, boolean showDbInPrompt) {
    BeeLine mockBeeLine = mock(BeeLine.class);
    DatabaseConnection mockDatabaseConnection = mock(DatabaseConnection.class);
    Connection mockConnection = mock(Connection.class);
    try {
      when(mockConnection.getSchema()).thenReturn("newDatabase");
      when(mockDatabaseConnection.getConnection()).thenReturn(mockConnection);
    } catch (SQLException sqlException) {
      // We do mnot test this
    }
    when(mockBeeLine.getDatabaseConnection()).thenReturn(mockDatabaseConnection);
    BeeLineOpts mockBeeLineOpts = mock(BeeLineOpts.class);
    when(mockBeeLineOpts.getShowDbInPrompt()).thenReturn(showDbInPrompt);
    when(mockBeeLine.getOpts()).thenReturn(mockBeeLineOpts);
    when(mockBeeLine.isBeeLine()).thenReturn(isBeeLine);

    return mockBeeLine;
  }

  @Test
  public void testGetHookBeeLineWithShowDbInPrompt() {
    BeeLine beeLine = setupMockData(true, true);
    Assertions.assertNull(ClientCommandHookFactory.get().getHook(beeLine, "set a;"));
    Assertions.assertNull(ClientCommandHookFactory.get().getHook(beeLine, "set a=b;"));
    Assertions.assertTrue(
        ClientCommandHookFactory.get().getHook(beeLine, "USE a.b")
            instanceof ClientCommandHookFactory.UseCommandHook);
    Assertions.assertTrue(
        ClientCommandHookFactory.get().getHook(beeLine, "coNNect a.b")
            instanceof ClientCommandHookFactory.ConnectCommandHook);
    Assertions.assertTrue(
        ClientCommandHookFactory.get().getHook(beeLine, "gO 1")
            instanceof ClientCommandHookFactory.GoCommandHook);
    Assertions.assertNull(ClientCommandHookFactory.get().getHook(beeLine, "g"));
  }

  @Test
  public void testGetHookBeeLineWithoutShowDbInPrompt() {
    BeeLine beeLine = setupMockData(true, false);
    Assertions.assertNull(ClientCommandHookFactory.get().getHook(beeLine, "set a;"));
    Assertions.assertNull(ClientCommandHookFactory.get().getHook(beeLine, "set a=b;"));
    Assertions.assertNull(ClientCommandHookFactory.get().getHook(beeLine, "USE a.b"));
    Assertions.assertNull(ClientCommandHookFactory.get().getHook(beeLine, "coNNect a.b"));
    Assertions.assertNull(ClientCommandHookFactory.get().getHook(beeLine, "gO 1"));
    Assertions.assertNull(ClientCommandHookFactory.get().getHook(beeLine, "g"));
  }

  @Test
  public void testUseHook() {
    BeeLine beeLine = setupMockData(true, true);
    ClientHook hook = ClientCommandHookFactory.get().getHook(beeLine, "USE newDatabase1");
    Assertions.assertTrue(hook instanceof ClientCommandHookFactory.UseCommandHook);
    hook.postHook(beeLine);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(beeLine).setCurrentDatabase(argument.capture());
    Assertions.assertEquals("newDatabase1", argument.getValue());
  }

  @Test
  public void testConnectHook() {
    BeeLine beeLine = setupMockData(true, true);
    ClientHook hook =
        ClientCommandHookFactory.get()
            .getHook(beeLine, "coNNect jdbc:hive2://localhost:10000/newDatabase2 a a");
    Assertions.assertTrue(hook instanceof ClientCommandHookFactory.ConnectCommandHook);
    hook.postHook(beeLine);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(beeLine).setCurrentDatabase(argument.capture());
    Assertions.assertEquals("newDatabase2", argument.getValue());
  }

  @Test
  public void testGoHook() {
    BeeLine beeLine = setupMockData(true, true);
    ClientHook hook = ClientCommandHookFactory.get().getHook(beeLine, "go 1");
    Assertions.assertTrue(hook instanceof ClientCommandHookFactory.GoCommandHook);
    hook.postHook(beeLine);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(beeLine).setCurrentDatabase(argument.capture());
    Assertions.assertEquals("newDatabase", argument.getValue());
  }
}
