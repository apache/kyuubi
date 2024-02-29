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

import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import org.apache.kyuubi.shaded.thrift.TConfiguration;
import org.apache.kyuubi.shaded.thrift.transport.TSSLTransportFactory;
import org.apache.kyuubi.shaded.thrift.transport.TSocket;
import org.apache.kyuubi.shaded.thrift.transport.TTransport;
import org.apache.kyuubi.shaded.thrift.transport.TTransportException;

/**
 * This class helps in some aspects of authentication. It creates the proper Thrift classes for the
 * given configuration as well as helps with authenticating requests.
 */
public class ThriftUtils {
  public static TTransport getSocketTransport(
      String host, int port, int connectTimeout, int socketTimeout) throws TTransportException {
    return new TSocket(TConfiguration.DEFAULT, host, port, socketTimeout, connectTimeout);
  }

  public static TTransport getSSLSocket(
      String host, int port, int connectTimeout, int socketTimeout) throws TTransportException {
    // The underlying SSLSocket object is bound to host:port with the given SO_TIMEOUT
    TSocket tSSLSocket = TSSLTransportFactory.getClientSocket(host, port, socketTimeout);
    tSSLSocket.setConnectTimeout(connectTimeout);
    return getSSLSocketWithHttps(tSSLSocket);
  }

  public static TTransport getSSLSocket(
      String host,
      int port,
      int connectTimeout,
      int socketTimeout,
      String trustStorePath,
      String trustStorePassWord)
      throws TTransportException {
    TSSLTransportFactory.TSSLTransportParameters params =
        new TSSLTransportFactory.TSSLTransportParameters();
    params.setTrustStore(trustStorePath, trustStorePassWord);
    params.requireClientAuth(true);
    // The underlying SSLSocket object is bound to host:port with the given SO_TIMEOUT and
    // SSLContext created with the given params
    TSocket tSSLSocket = TSSLTransportFactory.getClientSocket(host, port, socketTimeout, params);
    tSSLSocket.setConnectTimeout(connectTimeout);
    return getSSLSocketWithHttps(tSSLSocket);
  }

  // Using endpoint identification algorithm as HTTPS enables us to do
  // CNAMEs/subjectAltName verification
  private static TSocket getSSLSocketWithHttps(TSocket tSSLSocket) throws TTransportException {
    SSLSocket sslSocket = (SSLSocket) tSSLSocket.getSocket();
    SSLParameters sslParams = sslSocket.getSSLParameters();
    sslParams.setEndpointIdentificationAlgorithm("HTTPS");
    sslSocket.setSSLParameters(sslParams);
    return new TSocket(sslSocket);
  }
}
