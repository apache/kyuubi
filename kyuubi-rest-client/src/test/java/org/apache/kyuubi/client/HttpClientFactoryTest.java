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

package org.apache.kyuubi.client;

import static org.apache.kyuubi.client.RestClientTestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.security.KeyStore;
import java.security.cert.CertPathValidatorException;
import java.util.List;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kyuubi.client.api.v1.dto.Batch;
import org.apache.kyuubi.client.api.v1.dto.BatchRequest;
import org.apache.kyuubi.client.exception.KyuubiRestException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class HttpClientFactoryTest {

  private static final String TRUST_STORE_TYPE_PROPERTY = "javax.net.ssl.trustStoreType";
  private static final String TRUST_STORE_PROPERTY = "javax.net.ssl.trustStore";
  private static final String TRUST_STORE_PASSWORD_PROPERTY = "javax.net.ssl.trustStorePassword";
  private static final String TRUST_STORE_PASSWORD = "trust_store_password";

  private ServerTestHelper serverTestHelper;

  @BeforeEach
  public void setUp() throws Exception {
    serverTestHelper = new ServerTestHelper();
    serverTestHelper.setup(BatchTestServlet.class);
    BatchTestServlet.setAuthSchema(BASIC_AUTH);
    BatchTestServlet.allowAnonymous(false);
  }

  @AfterEach
  public void tearDown() throws Exception {
    serverTestHelper.stop();
  }

  @Test
  public void testDefaultSslVerificationRejectsUntrustedServerCertificate() throws Exception {
    // the test server certificate is self-signed, so the chain is not trusted
    try (KyuubiRestClient client =
        KyuubiRestClient.builder("https://localhost:8443")
            .authHeaderMethod(KyuubiRestClient.AuthHeaderMethod.BASIC)
            .username(TEST_USERNAME)
            .password(TEST_PASSWORD)
            .build()) {
      BatchRestApi batchRestApi = new BatchRestApi(client);
      KyuubiRestException exception =
          assertThrows(KyuubiRestException.class, () -> batchRestApi.getBatchById("71535"));
      assertTrue(
          ExceptionUtils.getThrowableList(exception).stream()
              .anyMatch(cause -> cause instanceof SSLHandshakeException),
          "expected the TLS handshake to reject the untrusted server certificate, but got: "
              + exception);
    }
  }

  @Test
  public void testDefaultSslVerificationRejectsMismatchedHostname() throws Exception {
    // trust the server certificate, so that only the hostname check can reject the connection
    System.setProperty(TRUST_STORE_PROPERTY, writeTrustStore().getAbsolutePath());
    System.setProperty(TRUST_STORE_TYPE_PROPERTY, "JKS");
    System.setProperty(TRUST_STORE_PASSWORD_PROPERTY, TRUST_STORE_PASSWORD);
    try (KyuubiRestClient client =
        KyuubiRestClient.builder("https://localhost:8443")
            .authHeaderMethod(KyuubiRestClient.AuthHeaderMethod.BASIC)
            .username(TEST_USERNAME)
            .password(TEST_PASSWORD)
            .build()) {
      BatchRestApi batchRestApi = new BatchRestApi(client);
      KyuubiRestException exception =
          assertThrows(KyuubiRestException.class, () -> batchRestApi.getBatchById("71535"));
      List<Throwable> causes = ExceptionUtils.getThrowableList(exception);
      assertTrue(
          causes.stream().anyMatch(cause -> cause instanceof SSLPeerUnverifiedException),
          "expected the hostname verification to reject the server certificate, but got: "
              + causes);
      assertFalse(
          causes.stream().anyMatch(cause -> cause instanceof CertPathValidatorException),
          "expected the server certificate to be trusted, but got: " + causes);
    } finally {
      System.clearProperty(TRUST_STORE_PROPERTY);
      System.clearProperty(TRUST_STORE_TYPE_PROPERTY);
      System.clearProperty(TRUST_STORE_PASSWORD_PROPERTY);
    }
  }

  @Test
  public void testInsecureSslAcceptsUntrustedServerCertificate() throws Exception {
    try (KyuubiRestClient client =
        KyuubiRestClient.builder("https://localhost:8443")
            .authHeaderMethod(KyuubiRestClient.AuthHeaderMethod.BASIC)
            .username(TEST_USERNAME)
            .password(TEST_PASSWORD)
            .insecureSsl(true)
            .build()) {
      BatchRestApi batchRestApi = new BatchRestApi(client);
      BatchRequest batchRequest = generateTestBatchRequest();
      Batch expectedBatch = generateTestBatch();
      Batch result = batchRestApi.createBatch(batchRequest);
      assertEquals(expectedBatch.getId(), result.getId());
    }
  }

  private File writeTrustStore() throws Exception {
    KeyStore trustStore = KeyStore.getInstance("JKS");
    trustStore.load(null, null);
    trustStore.setCertificateEntry("kyuubi-test", serverTestHelper.getServerCertificate());

    File trustStoreFile =
        new File(System.getProperty("test.dir", "target"), "kyuubi-test-truststore.jks");
    try (FileOutputStream outputStream = new FileOutputStream(trustStoreFile)) {
      trustStore.store(outputStream, TRUST_STORE_PASSWORD.toCharArray());
    }
    return trustStoreFile;
  }
}
