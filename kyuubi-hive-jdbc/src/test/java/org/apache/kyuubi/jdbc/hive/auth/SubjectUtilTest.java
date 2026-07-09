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

package org.apache.kyuubi.jdbc.hive.auth;

import static org.junit.jupiter.api.Assertions.*;

import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.CompletionException;
import javax.security.auth.Subject;
import org.junit.jupiter.api.Test;

public class SubjectUtilTest {

  @Test
  void current_returnsNullWhenNoSubjectAssociated() {
    assertNull(assertDoesNotThrow(SubjectUtil::current));
  }

  @Test
  void callAs_runsActionAndReturnsResult() {
    Subject subject = new Subject();
    String result = SubjectUtil.callAs(subject, () -> "hello");
    assertEquals("hello", result);
  }

  @Test
  void callAs_wrapsCheckedExceptionInCompletionException() {
    Subject subject = new Subject();
    Exception cause = new Exception("oops");
    CompletionException ex =
        assertThrows(
            CompletionException.class,
            () ->
                SubjectUtil.callAs(
                    subject,
                    () -> {
                      throw cause;
                    }));
    assertSame(cause, ex.getCause());
  }

  @Test
  void doAs_privilegedAction_runsAndReturnsResult() {
    Subject subject = new Subject();
    String result = SubjectUtil.doAs(subject, (PrivilegedAction<String>) () -> "world");
    assertEquals("world", result);
  }

  @Test
  void doAs_privilegedExceptionAction_propagatesCheckedException() {
    Subject subject = new Subject();
    Exception cause = new Exception("checked");
    assertThrows(
        Exception.class,
        () ->
            SubjectUtil.doAs(
                subject,
                (PrivilegedExceptionAction<Void>)
                    () -> {
                      throw cause;
                    }));
  }
}
