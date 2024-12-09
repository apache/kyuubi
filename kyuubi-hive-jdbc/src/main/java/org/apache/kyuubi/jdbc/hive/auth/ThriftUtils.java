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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class helps in some aspects of authentication. It creates the proper Thrift classes for the
 * given configuration as well as helps with authenticating requests.
 */
public class ThriftUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ThriftUtils.class);

  /**
   * Configure the provided T transport's max message size.
   *
   * @param transport Transport to configure maxMessage for
   * @param maxMessageSize Maximum allowed message size in bytes, less than or equal to 0 means use
   *     the Thrift library default.
   * @return The passed in T transport configured with desired max message size. The same object
   *     passed in is returned.
   */
  public static <T extends TTransport> T configureThriftMaxMessageSize(
      T transport, int maxMessageSize) {
    if (maxMessageSize > 0) {
      if (transport.getConfiguration() == null) {
        LOG.warn(
            "TTransport {} is returning a null Configuration, Thrift max message size is not getting configured",
            transport.getClass().getName());
        return transport;
      }
      transport.getConfiguration().setMaxMessageSize(maxMessageSize);
    }
    return transport;
  }

  /**
   * Create a TSocket for the provided host and port with specified connectTimeout, loginTimeout and
   * maxMessageSize.
   *
   * @param host Host to connect to.
   * @param port Port to connect to.
   * @param connectTimeout Socket connect timeout (0 means no timeout).
   * @param socketTimeout Socket read/write timeout (0 means no timeout).
   * @param maxMessageSize Size in bytes for max allowable Thrift message size, less than or equal
   *     to 0 results in using the Thrift library default.
   * @return TTransport TSocket for host/port
   */
  public static TTransport getSocketTransport(
      String host, int port, int connectTimeout, int socketTimeout, int maxMessageSize)
      throws TTransportException {
    TConfiguration.Builder tConfBuilder = TConfiguration.custom();
    if (maxMessageSize > 0) {
      tConfBuilder.setMaxMessageSize(maxMessageSize);
    }
    TConfiguration tConf = tConfBuilder.build();
    return new TSocket(tConf, host, port, socketTimeout, connectTimeout);
  }

  public static TTransport getSSLSocket(
      String host, int port, int connectTimeout, int socketTimeout, int maxMessageSize)
      throws TTransportException {
    // The underlying SSLSocket object is bound to host:port with the given SO_TIMEOUT
    TSocket tSSLSocket = TSSLTransportFactory.getClientSocket(host, port, socketTimeout);
    tSSLSocket.setConnectTimeout(connectTimeout);
    return getSSLSocketWithHttps(tSSLSocket, maxMessageSize);
  }

  public static TTransport getSSLSocket(
      String host,
      int port,
      int connectTimeout,
      int socketTimeout,
      String trustStorePath,
      String trustStorePassWord,
      int maxMessageSize)
      throws TTransportException {
    TSSLTransportFactory.TSSLTransportParameters params =
        new TSSLTransportFactory.TSSLTransportParameters();
    params.setTrustStore(trustStorePath, trustStorePassWord);
    params.requireClientAuth(true);
    // The underlying SSLSocket object is bound to host:port with the given SO_TIMEOUT and
    // SSLContext created with the given params
    TSocket tSSLSocket = TSSLTransportFactory.getClientSocket(host, port, socketTimeout, params);
    tSSLSocket.setConnectTimeout(connectTimeout);
    return getSSLSocketWithHttps(tSSLSocket, maxMessageSize);
  }

  // Using endpoint identification algorithm as HTTPS enables us to do
  // CNAMEs/subjectAltName verification
  private static TSocket getSSLSocketWithHttps(TSocket tSSLSocket, int maxMessageSize)
      throws TTransportException {
    SSLSocket sslSocket = (SSLSocket) tSSLSocket.getSocket();
    SSLParameters sslParams = sslSocket.getSSLParameters();
    sslParams.setEndpointIdentificationAlgorithm("HTTPS");
    sslSocket.setSSLParameters(sslParams);
    TSocket tSocket = new TSocket(sslSocket);
    return configureThriftMaxMessageSize(tSocket, maxMessageSize);
  }
}
