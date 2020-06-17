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

package org.apache.kyuubi.spark;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.kyuubi.util.ExecutorPoolCaptureOom;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

import javax.net.ssl.SSLServerSocket;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class KyuubiThriftBinaryCliService extends ThriftCLIService {

  public KyuubiThriftBinaryCliService(CLIService cliService) {
    super(cliService, KyuubiThriftBinaryCliService.class.getSimpleName());
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
    super.init(hiveConf);
    // a hook stop the app when oom occurs
    Runnable hook = new Runnable() {
      @Override
      public void run() {
        stop();
      }
    };
    try {
      ExecutorPoolCaptureOom executorService = new ExecutorPoolCaptureOom(
        "threadPoolName",
        minWorkerThreads,
        maxWorkerThreads,
        workerKeepAliveTime, hook);
      hiveAuthFactory = new HiveAuthFactory(hiveConf);
      TTransportFactory transportFactory = hiveAuthFactory.getAuthTransFactory();
      TProcessorFactory processorFactory = hiveAuthFactory.getAuthProcFactory(this);
      TServerSocket serverSocket = null;
      List<String> sslVersionBlacklist = new ArrayList<String>();
      if (!hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_USE_SSL)) {
        serverSocket = getServerSocket(hiveHost, portNum);
      } else {
        String keyStorePath = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH).trim();
        if (keyStorePath.isEmpty()) {
          throw new IllegalArgumentException(HiveConf.ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH.varname
            + " Not configured for SSL connection");
        }
        String keyStorePassword = ShimLoader.getHadoopShims().getPassword(hiveConf,
          HiveConf.ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PASSWORD.varname);
        serverSocket = getServerSSLSocket(hiveHost, portNum, keyStorePath,
          keyStorePassword, sslVersionBlacklist);
      }

      portNum = serverSocket.getServerSocket().getLocalPort();

      // Server args
      int maxMessageSize = hiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_MAX_MESSAGE_SIZE);
      int requestTimeout = (int) hiveConf.getTimeVar(
        HiveConf.ConfVars.HIVE_SERVER2_THRIFT_LOGIN_TIMEOUT, TimeUnit.SECONDS);
      int beBackoffSlotLength = (int) hiveConf.getTimeVar(
        HiveConf.ConfVars.HIVE_SERVER2_THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH, TimeUnit.MILLISECONDS);
      TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(serverSocket)
        .processorFactory(processorFactory).transportFactory(transportFactory)
        .protocolFactory(new TBinaryProtocol.Factory())
        .inputProtocolFactory(new TBinaryProtocol.Factory(true, true, maxMessageSize, maxMessageSize))
        .requestTimeout(requestTimeout).requestTimeoutUnit(TimeUnit.SECONDS)
        .beBackoffSlotLength(beBackoffSlotLength).beBackoffSlotLengthUnit(TimeUnit.MILLISECONDS)
        .executorService(executorService);

      // TCP Server
      server = new TThreadPoolServer(sargs);
      server.setServerEventHandler(serverEventHandler);
      String msg = "Starting " + getName() + " on port "
        + portNum + " with " + minWorkerThreads + "..." + maxWorkerThreads + " worker threads";
      LOG.info(msg);
    } catch (Throwable t) {
      LOG.error("Error starting" + getName(), t);
      stop();
    }
  }

  protected void initializeServer() {}

  @Override
  public void run() {
    try {
      server.serve();
    } catch (Throwable t) {
      LOG.error("Error starting" + getName(), t);
      stop();
    }
  }

  public static TServerSocket getServerSocket(String hiveHost, int portNum)
    throws TTransportException {
    InetSocketAddress serverAddress;
    if (hiveHost == null || hiveHost.isEmpty()) {
      // Wildcard bind
      serverAddress = new InetSocketAddress(portNum);
    } else {
      serverAddress = new InetSocketAddress(hiveHost, portNum);
    }
    return new TServerSocket(serverAddress);
  }

  public static TServerSocket getServerSSLSocket(String hiveHost, int portNum, String keyStorePath,
      String keyStorePassWord, List<String> sslVersionBlacklist) throws TTransportException, UnknownHostException {
    TSSLTransportFactory.TSSLTransportParameters params =
      new TSSLTransportFactory.TSSLTransportParameters();
    params.setKeyStore(keyStorePath, keyStorePassWord);
    InetSocketAddress serverAddress;
    if (hiveHost == null || hiveHost.isEmpty()) {
      // Wildcard bind
      serverAddress = new InetSocketAddress(portNum);
    } else {
      serverAddress = new InetSocketAddress(hiveHost, portNum);
    }
    TServerSocket thriftServerSocket =
      TSSLTransportFactory.getServerSocket(portNum, 0, serverAddress.getAddress(), params);
    if (thriftServerSocket.getServerSocket() instanceof SSLServerSocket) {
      List<String> sslVersionBlacklistLocal = new ArrayList<String>();
      for (String sslVersion : sslVersionBlacklist) {
        sslVersionBlacklistLocal.add(sslVersion.trim().toLowerCase());
      }
      SSLServerSocket sslServerSocket = (SSLServerSocket) thriftServerSocket.getServerSocket();
      List<String> enabledProtocols = new ArrayList<String>();
      for (String protocol : sslServerSocket.getEnabledProtocols()) {
        if (sslVersionBlacklistLocal.contains(protocol.toLowerCase())) {
          LOG.debug("Disabling SSL Protocol: " + protocol);
        } else {
          enabledProtocols.add(protocol);
        }
      }
      sslServerSocket.setEnabledProtocols(enabledProtocols.toArray(new String[0]));
      LOG.info("SSL Server Socket Enabled Protocols: "
        + Arrays.toString(sslServerSocket.getEnabledProtocols()));
    }
    return thriftServerSocket;
  }
}
