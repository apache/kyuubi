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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.protocol.BasicHttpContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class HttpBasicAuthInterceptorTest {

  @ParameterizedTest
  @CsvSource({
    "testaccount,password123!",
    "testaccount,§",
    "testaccount,ß",
    "testaccount,ö",
    "testaccount,ä",
    "testaccount,ü",
    "testaccount,€",
    "testaccount,§ßöäü€",
    "töstuser,§ßöäü€"
  })
  public void preservesUtf8Credentials(String username, String password) throws Exception {
    HttpBasicAuthInterceptor interceptor =
        new HttpBasicAuthInterceptor(username, password, null, null, false, null, null);
    BasicHttpRequest request = new BasicHttpRequest("POST", "/cliservice");
    interceptor.process(request, new BasicHttpContext());

    String authorization = request.getFirstHeader("Authorization").getValue();
    assertEquals("Basic ", authorization.substring(0, 6));
    // The server decodes HTTP Basic credentials as UTF-8.
    String credentials =
        new String(Base64.getDecoder().decode(authorization.substring(6)), StandardCharsets.UTF_8);
    assertEquals(username + ":" + password, credentials);
  }
}
