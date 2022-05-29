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
import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;

public class KerberizedTestHelper {

  private MiniKdc kdc;
  private File workDir = new File(System.getProperty("test.dir", "target"));
  private Properties conf = MiniKdc.createConf();

  private File keytabFile;
  protected String keytabFilePath;
  protected String clientPrincipal = "client/localhost";
  protected String spnegoPrincipal = "HTTP/localhost";

  public void setup() throws Exception {
    kdc = new MiniKdc(conf, workDir);
    kdc.start();

    keytabFile = new File(workDir, "kyuubi-test.keytab");
    keytabFilePath = keytabFile.getAbsolutePath();
    kdc.createPrincipal(keytabFile, clientPrincipal, spnegoPrincipal);

    Configuration conf = new Configuration();
    conf.setBoolean("hadoop.security.authorization", true);
    conf.set("hadoop.security.authentication", "Kerberos");
    UserGroupInformation.setConfiguration(conf);
  }

  public void stop() {
    if (kdc != null) {
      kdc.stop();
    }
  }

  public void login() throws IOException {
    UserGroupInformation.loginUserFromKeytab(clientPrincipal, keytabFilePath);
  }
}
