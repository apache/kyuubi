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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class KyuubiStatementTest {

  @Test
  public void testSetFetchSize1() throws SQLException {
    try (KyuubiStatement stmt = new KyuubiStatement(null, null, null)) {
      stmt.setFetchSize(123);
      assertEquals(123, stmt.getFetchSize());
    }
  }

  @Test
  public void testSetFetchSize2() throws SQLException {
    try (KyuubiStatement stmt = new KyuubiStatement(null, null, null)) {
      int initial = stmt.getFetchSize();
      stmt.setFetchSize(0);
      assertEquals(initial, stmt.getFetchSize());
    }
  }

  @Test(expected = SQLException.class)
  public void testSetFetchSize3() throws SQLException {
    try (KyuubiStatement stmt = new KyuubiStatement(null, null, null)) {
      stmt.setFetchSize(-1);
    }
  }

  @Test(expected = SQLException.class)
  public void testaddBatch() throws SQLException {
    try (KyuubiStatement stmt = new KyuubiStatement(null, null, null)) {
      stmt.addBatch(null);
    }
  }

  @Test
  public void testThrowKyuubiSQLExceptionWhenExecuteSqlOnClosedStmt()
      throws SQLException, InterruptedException {
    try (KyuubiStatement stmt = new KyuubiStatement(null, null, null)) {
      AtomicReference<Throwable> assertionFailure = new AtomicReference<>();
      CountDownLatch latch = new CountDownLatch(1);

      Thread thread1 =
          new Thread(
              () -> {
                try {
                  latch.countDown();
                  stmt.close();
                } catch (SQLException e) {
                  assertionFailure.set(e);
                }
              });

      Thread thread2 =
          new Thread(
              () -> {
                try {
                  latch.await();
                  KyuubiSQLException ex =
                      assertThrows(KyuubiSQLException.class, () -> stmt.execute("SELECT 1"));
                  assertEquals("Can't execute after statement has been closed", ex.getMessage());
                } catch (Throwable t) {
                  assertionFailure.set(t);
                }
              });

      thread1.start();
      thread2.start();

      thread1.join();
      thread2.join();

      if (assertionFailure.get() != null) {
        throw new AssertionError("Assertion failed in thread", assertionFailure.get());
      }
    }
  }
}
