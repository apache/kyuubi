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

package org.apache.kyuubi.flink.security.token.utils;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KyuubiUtils {

  private static final Logger LOG = LoggerFactory.getLogger(KyuubiUtils.class);

  public static final String KYUUBI_ENGINE_CREDENTIALS_KEY = "kyuubi.engine.credentials";
  public static final String KYUUBI_CREDENTIALS_RENEWAL_INTERVAL_KEY =
      "kyuubi.credentials.renewal.interval";
  public static final Long KYUUBI_CREDENTIALS_RENEWAL_INTERVAL_DEFAULT = 360000L;

  // org.apache.kyuubi.Utils.fromCommandLineArgs
  public static Map<String, String> fromCommandLineArgs(List<String> args) {
    assert args.size() % 2 == 0 : "Illegal size of arguments.";

    Map<String, String> conf = new HashMap<>();
    for (int i = 0; i < args.size(); i++) {
      String confKey = args.get(i);
      String confValues = args.get(++i);
      assert confKey.equals("--conf")
          : "Unrecognized main arguments prefix "
              + confKey
              + ", the argument format is '--conf k=v'.";
      String[] confItem = confValues.split("=", 2);
      if (confItem.length == 2) {
        conf.put(confItem[0].trim(), confItem[1].trim());
      } else {
        throw new IllegalArgumentException("Illegal argument: " + confValues + ".");
      }
    }

    return conf;
  }

  public static void renewDelegationToken(String delegationToken)
      throws NoSuchFieldException, IllegalAccessException, IOException {
    Credentials newCreds = decodeCredentials(delegationToken);
    Map<Text, Token<? extends TokenIdentifier>> newTokens = getTokenMap(newCreds);

    Credentials updateCreds = new Credentials();
    Credentials oldCreds = UserGroupInformation.getCurrentUser().getCredentials();
    for (Map.Entry<Text, Token<? extends TokenIdentifier>> entry : newTokens.entrySet()) {
      Text alias = entry.getKey();
      Token<? extends TokenIdentifier> newToken = entry.getValue();
      Token<? extends TokenIdentifier> oldToken = oldCreds.getToken(entry.getKey());
      if (oldToken != null) {
        if (compareIssueDate(newToken, oldToken) > 0) {
          updateCreds.addToken(alias, newToken);
        } else {
          LOG.warn("Ignore token with earlier issue date: {}", newToken);
        }
      } else {
        LOG.info("Add new unknown token {}", newToken);
        updateCreds.addToken(alias, newToken);
      }
    }

    if (updateCreds.numberOfTokens() > 0) {
      LOG.info(
          "Update delegation tokens. The number of tokens sent by the server is {}. "
              + "The actual number of updated tokens is {}.",
          newCreds.numberOfTokens(),
          updateCreds.numberOfTokens());
      UserGroupInformation.getCurrentUser().addCredentials(updateCreds);
    }
  }

  public static Credentials decodeCredentials(String newValue) throws IOException {
    byte[] decoded = Base64.getDecoder().decode(newValue);
    ByteArrayInputStream byteStream = new ByteArrayInputStream(decoded);
    Credentials creds = new Credentials();
    creds.readTokenStorageStream(new DataInputStream(byteStream));
    return creds;
  }

  /**
   * Get [[Credentials#tokenMap]] by reflection as [[Credentials#getTokenMap]] is not present before
   * Hadoop 3.2.1.
   */
  public static Map<Text, Token<? extends TokenIdentifier>> getTokenMap(Credentials credentials)
      throws NoSuchFieldException, IllegalAccessException {
    Field tokenMap = Credentials.class.getDeclaredField("tokenMap");
    tokenMap.setAccessible(true);
    return (Map<Text, Token<? extends TokenIdentifier>>) tokenMap.get(credentials);
  }

  public static int compareIssueDate(
      Token<? extends TokenIdentifier> newToken, Token<? extends TokenIdentifier> oldToken)
      throws IOException {
    Optional<Long> newDate = getTokenIssueDate(newToken);
    Optional<Long> oldDate = getTokenIssueDate(oldToken);
    if (newDate.isPresent() && oldDate.isPresent() && newDate.get() <= oldDate.get()) {
      return -1;
    } else {
      return 1;
    }
  }

  public static Optional<Long> getTokenIssueDate(Token<? extends TokenIdentifier> token)
      throws IOException {
    TokenIdentifier identifier = token.decodeIdentifier();
    if (identifier instanceof AbstractDelegationTokenIdentifier) {
      return Optional.of(((AbstractDelegationTokenIdentifier) identifier).getIssueDate());
    }
    if (identifier == null) {
      // TokenIdentifiers not found in ServiceLoader
      DelegationTokenIdentifier tokenIdentifier = new DelegationTokenIdentifier();
      ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
      DataInputStream in = new DataInputStream(buf);
      try {
        tokenIdentifier.readFields(in);
        return Optional.of(tokenIdentifier.getIssueDate());
      } catch (Exception e) {
        LOG.warn("Can not decode identifier of token {}, error: {}", token, e);
        return Optional.empty();
      }
    }
    LOG.debug("Unsupported TokenIdentifier kind: {}", identifier.getKind());
    return Optional.empty();
  }
}
