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

import java.io.File;
import java.io.FileOutputStream;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Date;
import javax.servlet.Servlet;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import sun.security.tools.keytool.CertAndKeyGen;
import sun.security.x509.X500Name;

public class ServerTestHelper {

  private String password = "my_password";
  private File keyStoreFile = new File(System.getProperty("test.dir", "target"), "kyuubi-test.jks");

  private Server server;

  private int maxThreads = 100;
  private int minThreads = 10;
  private int idleTimeout = 120;

  public void setup(Class<? extends Servlet> servlet) throws Exception {
    System.setProperty("jdk.tls.server.protocols", "TLSv1.2");
    System.setProperty("jdk.tls.client.protocols", "TLSv1.2");

    generateKeyStore();
    setupServer(servlet);
  }

  public void stop() throws Exception {
    if (server != null) {
      server.stop();
    }
  }

  private void generateKeyStore() throws Exception {
    KeyStore ks = KeyStore.getInstance("jks");
    ks.load(null, null);

    CertAndKeyGen keypair = new CertAndKeyGen("RSA", "SHA1WithRSA", null);
    X500Name x500Name = new X500Name("", "", "", "", "", "");
    keypair.generate(1024);

    PrivateKey privateKey = keypair.getPrivateKey();
    X509Certificate[] chain = new X509Certificate[1];
    chain[0] = keypair.getSelfCertificate(x500Name, new Date(), (long) 24 * 60 * 60);

    File keyStoreFile = new File(System.getProperty("test.dir", "target"), "kyuubi-test.jks");
    // store away the key store
    FileOutputStream fos = new FileOutputStream(keyStoreFile);
    ks.setKeyEntry("kyuubi-test", privateKey, password.toCharArray(), chain);
    ks.store(fos, password.toCharArray());
    fos.close();
  }

  private void setupServer(Class<? extends Servlet> servlet) throws Exception {
    QueuedThreadPool threadPool = new QueuedThreadPool(maxThreads, minThreads, idleTimeout);
    server = new Server(threadPool);

    ServerConnector connector = setupHttpConnector();
    ServerConnector sslConnector = setupHttpsConnector();
    server.setConnectors(new Connector[] {connector, sslConnector});

    ServletHandler servletHandler = new ServletHandler();
    server.setHandler(servletHandler);
    servletHandler.addServletWithMapping(servlet, "/*");

    server.start();
  }

  private ServerConnector setupHttpConnector() {
    // HTTP Configuration
    HttpConfiguration http = new HttpConfiguration();
    http.addCustomizer(new SecureRequestCustomizer());

    // Configuration for HTTPS redirect
    http.setSecurePort(8443);
    http.setSecureScheme("https");

    ServerConnector connector = new ServerConnector(server);
    connector.addConnectionFactory(new HttpConnectionFactory(http));
    // Setting HTTP port
    connector.setPort(8080);

    return connector;
  }

  private ServerConnector setupHttpsConnector() {
    // HTTPS Configuration
    HttpConfiguration https = new HttpConfiguration();
    https.addCustomizer(new SecureRequestCustomizer());

    // Configuring SSL
    SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
    sslContextFactory.setKeyStorePath(keyStoreFile.getAbsolutePath());
    sslContextFactory.setTrustStorePath(keyStoreFile.getAbsolutePath());
    sslContextFactory.setKeyStorePassword(password);
    sslContextFactory.setKeyManagerPassword(password);
    sslContextFactory.setTrustStorePassword(password);

    // Configuring the connector
    ServerConnector sslConnector =
        new ServerConnector(
            server,
            new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString()),
            new HttpConnectionFactory(https));
    sslConnector.setPort(8443);

    return sslConnector;
  }
}
