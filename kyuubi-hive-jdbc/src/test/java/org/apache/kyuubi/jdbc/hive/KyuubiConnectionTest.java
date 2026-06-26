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
package org.apache.kyuubi.jdbc.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TCLIService.Iface;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TCloseSessionReq;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TGetOperationStatusReq;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TGetOperationStatusResp;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.THandleIdentifier;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TOperationHandle;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TOperationState;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TOperationType;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TSessionHandle;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TStatus;
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TStatusCode;
import org.apache.kyuubi.shaded.thrift.TException;
import org.junit.jupiter.api.Test;
import org.objenesis.ObjenesisStd;

public class KyuubiConnectionTest {

  @Test
  public void waitLaunchEngineToCompleteHonorsInterrupt() throws Exception {
    Iface client = mock(Iface.class);
    TGetOperationStatusResp finishedResp = new TGetOperationStatusResp();
    finishedResp.setStatus(new TStatus(TStatusCode.SUCCESS_STATUS));
    finishedResp.setOperationState(TOperationState.FINISHED_STATE);
    when(client.GetOperationStatus(any(TGetOperationStatusReq.class))).thenReturn(finishedResp);

    Thread engineLogThread =
        new Thread(
            () -> {
              try {
                Thread.sleep(Long.MAX_VALUE);
              } catch (InterruptedException ignored) {
              }
            });
    engineLogThread.setDaemon(true);
    engineLogThread.start();

    KyuubiConnection connection = newKyuubiConnection(client, engineLogThread);

    try {
      Thread.currentThread().interrupt();

      KyuubiInterruptedException exception =
          assertThrows(KyuubiInterruptedException.class, connection::waitLaunchEngineToComplete);

      assertEquals("01000", exception.getSQLState());
      assertTrue(hasCause(exception, InterruptedException.class));
      assertFalse(Thread.currentThread().isInterrupted());
      verify(client).CloseSession(any(TCloseSessionReq.class));
    } finally {
      Thread.interrupted();
      engineLogThread.interrupt();
      engineLogThread.join(1000);
    }
  }

  @Test
  public void waitLaunchEngineToCompleteMapsInterruptedCauseToCancel() throws Exception {
    Iface client = mock(Iface.class);
    when(client.GetOperationStatus(any(TGetOperationStatusReq.class)))
        .thenThrow(new TException("interrupted", new InterruptedException("interrupted")));
    KyuubiConnection connection = newKyuubiConnection(client, null);

    try {
      KyuubiInterruptedException exception =
          assertThrows(KyuubiInterruptedException.class, connection::waitLaunchEngineToComplete);

      assertEquals("01000", exception.getSQLState());
      assertTrue(hasCause(exception, InterruptedException.class));
      assertFalse(Thread.currentThread().isInterrupted());
      verify(client).CloseSession(any(TCloseSessionReq.class));
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  public void waitLaunchEngineToCompleteClearsCleanupInterruptForLaunchCancel() throws Exception {
    Iface client = mock(Iface.class);
    AtomicBoolean keepAlive = new AtomicBoolean(true);
    Thread engineLogThread =
        new Thread(
            () -> {
              while (keepAlive.get()) {
                try {
                  Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
              }
            });
    engineLogThread.setDaemon(true);
    engineLogThread.start();

    KyuubiConnection connection = newKyuubiConnection(client, engineLogThread);
    Thread currentThread = Thread.currentThread();
    Thread cleanupInterrupter =
        new Thread(
            () -> {
              try {
                Thread.sleep(50);
              } catch (InterruptedException ignored) {
              }
              currentThread.interrupt();
            });

    int originalEngineLogThreadTimeout = KyuubiConnection.DEFAULT_ENGINE_LOG_THREAD_TIMEOUT;
    try {
      KyuubiConnection.DEFAULT_ENGINE_LOG_THREAD_TIMEOUT = 1000;
      Thread.currentThread().interrupt();
      cleanupInterrupter.start();

      assertThrows(KyuubiInterruptedException.class, connection::waitLaunchEngineToComplete);

      assertFalse(Thread.currentThread().isInterrupted());
      verify(client).CloseSession(any(TCloseSessionReq.class));
    } finally {
      KyuubiConnection.DEFAULT_ENGINE_LOG_THREAD_TIMEOUT = originalEngineLogThreadTimeout;
      Thread.interrupted();
      cleanupInterrupter.join(1000);
      keepAlive.set(false);
      engineLogThread.interrupt();
      engineLogThread.join(1000);
    }
  }

  private static KyuubiConnection newKyuubiConnection(Iface client, Thread engineLogThread)
      throws Exception {
    KyuubiConnection connection =
        (KyuubiConnection) new ObjenesisStd().newInstance(KyuubiConnection.class);
    setField(connection, "client", client);
    setField(connection, "sessHandle", new TSessionHandle());
    setField(connection, "isClosed", false);
    setField(connection, "engineLogThread", engineLogThread);
    setField(connection, "launchEngineOpHandle", newOperationHandle());
    return connection;
  }

  private static TOperationHandle newOperationHandle() {
    THandleIdentifier identifier =
        new THandleIdentifier(ByteBuffer.wrap(new byte[16]), ByteBuffer.wrap(new byte[16]));
    return new TOperationHandle(identifier, TOperationType.UNKNOWN, false);
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static boolean hasCause(Throwable throwable, Class<? extends Throwable> causeClass) {
    Throwable cause = throwable;
    while (cause != null) {
      if (causeClass.isInstance(cause)) {
        return true;
      }
      cause = cause.getCause();
    }
    return false;
  }
}
