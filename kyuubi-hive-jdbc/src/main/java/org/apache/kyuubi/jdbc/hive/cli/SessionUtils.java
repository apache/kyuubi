/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.jdbc.hive.cli;

import java.io.IOException;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSelector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;

public class SessionUtils {
  /**
   * Get the string form of the token given a token signature. The signature is used as the value of
   * the "service" field in the token for lookup. Ref: AbstractDelegationTokenSelector in Hadoop. If
   * there exists such a token in the token cache (credential store) of the job, the lookup returns
   * that. This is relevant only when running against a "secure" hadoop release The method gets hold
   * of the tokens if they are set up by hadoop - this should happen on the map/reduce tasks if the
   * client added the tokens into hadoop's credential store in the front end during job submission.
   * The method will select the hive delegation token among the set of tokens and return the string
   * form of it
   *
   * @param tokenSignature
   * @return the string form of the token found
   * @throws IOException
   */
  public static String getTokenStrForm(String tokenSignature) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    TokenSelector<? extends TokenIdentifier> tokenSelector = new DelegationTokenSelector();

    Token<? extends TokenIdentifier> token =
        tokenSelector.selectToken(
            tokenSignature == null ? new Text() : new Text(tokenSignature), ugi.getTokens());
    return token != null ? token.encodeToUrlString() : null;
  }
}
