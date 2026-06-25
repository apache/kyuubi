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

package org.apache.kyuubi.engine.dataagent.provider.chatcompletion;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class ChatCompletionProviderTest {

  @Test
  public void testSemicolonUserDetected() {
    assertTrue(
        ChatCompletionProvider.hasKyuubiUrlCredentials(
            "jdbc:kyuubi://host:10009/default;user=alice;password=secret"));
  }

  @Test
  public void testSemicolonUsernameNotTreatedAsKyuubiCredential() {
    assertFalse(
        ChatCompletionProvider.hasKyuubiUrlCredentials(
            "jdbc:kyuubi://host:10009/default;username=alice"));
  }

  @Test
  public void testPasswordOnlyDoesNotSuppressSessionUser() {
    assertFalse(
        ChatCompletionProvider.hasKyuubiUrlCredentials(
            "jdbc:kyuubi://host:10009/default;password=secret"));
  }

  @Test
  public void testQueryParamsNotTreatedAsCredentials() {
    assertFalse(
        ChatCompletionProvider.hasKyuubiUrlCredentials(
            "jdbc:kyuubi://host:10009/default?user=alice&password=pw"));
  }

  @Test
  public void testNoCredentials() {
    assertFalse(ChatCompletionProvider.hasKyuubiUrlCredentials("jdbc:kyuubi://host:10009/default"));
  }

  @Test
  public void testNullUrl() {
    assertFalse(ChatCompletionProvider.hasKyuubiUrlCredentials(null));
  }

  @Test
  public void testSemicolonSubstringNoMatch() {
    assertFalse(
        ChatCompletionProvider.hasKyuubiUrlCredentials(
            "jdbc:kyuubi://host:10009/default;username_flag=true"));
  }

  @Test
  public void testKerberosPrincipalDetected() {
    assertTrue(
        ChatCompletionProvider.resolvesToKerberos(
            "jdbc:hive2://host:10000/default;principal=hive/host@REALM.COM"));
  }

  @Test
  public void testKerberosServerPrincipalAliasDetected() {
    assertTrue(
        ChatCompletionProvider.resolvesToKerberos(
            "jdbc:kyuubi://host:10009/default;kyuubiServerPrincipal=kyuubi/host@REALM.COM"));
  }

  @Test
  public void testKerberosAuthModeDetected() {
    assertTrue(
        ChatCompletionProvider.resolvesToKerberos(
            "jdbc:hive2://host:10009/db;auth=KERBEROS;transportMode=binary"));
  }

  @Test
  public void testNonKerberosUrlNotDetected() {
    assertFalse(
        ChatCompletionProvider.resolvesToKerberos(
            "jdbc:kyuubi://host:10009/default;user=alice;password=secret"));
  }

  @Test
  public void testQueryParamPrincipalNotTreatedAsKerberos() {
    assertFalse(
        ChatCompletionProvider.resolvesToKerberos(
            "jdbc:kyuubi://host:10009/default?principal=hive/host@REALM.COM"));
  }

  @Test
  public void testKerberosNullUrl() {
    assertFalse(ChatCompletionProvider.resolvesToKerberos(null));
  }
}
