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

package org.apache.kyuubi.client.auth;

import java.util.Base64;
import org.apache.commons.lang3.StringUtils;

public class BasicAuthHeaderGenerator implements AuthHeaderGenerator {
  private String authHeader;

  private BasicAuthHeaderGenerator() {}

  public BasicAuthHeaderGenerator(String user, String password) {
    if (user == null) {
      this.authHeader = "";
    } else {
      password = StringUtils.isNotBlank(password) ? password : "";
      String authorization = String.format("%s:%s", user, password);
      this.authHeader = "BASIC " + Base64.getEncoder().encodeToString(authorization.getBytes());
    }
  }

  @Override
  public String generateAuthHeader() {
    return authHeader;
  }
}
