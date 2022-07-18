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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import javax.security.auth.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HadoopShim {

  public static final String KYUUBI_DELEGATION_TOKEN_KIND = "KYUUBI_DELEGATION_TOKEN";

  private static final Logger LOG = LoggerFactory.getLogger(HadoopShim.class);

  private static final String UGI_CLASS = "org.apache.hadoop.security.UserGroupInformation";
  private static final String TEXT_CLASS = "org.apache.hadoop.io.Text";
  private static final String TOKEN_CLASS = "org.apache.hadoop.security.token.Token";
  private static final String ABSTRACT_DELEGATION_TOKEN_SELECTOR_CLASS =
      "org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSelector";

  private static boolean hadoopClzAvailable;

  private HadoopShim() {}

  static {
    try {
      Class.forName(UGI_CLASS);
      Class.forName(TEXT_CLASS);
      Class.forName(TOKEN_CLASS);
      Class.forName(ABSTRACT_DELEGATION_TOKEN_SELECTOR_CLASS);
      hadoopClzAvailable = true;
    } catch (ClassNotFoundException | NoClassDefFoundError e) {
      hadoopClzAvailable = false;
      LOG.info("Hadoop class is not available");
    }
  }

  public static boolean hadoopClzAvailable() {
    return hadoopClzAvailable;
  }

  private static void ensureHadoopClzAvailable() {
    if (!hadoopClzAvailable) {
      throw new IllegalStateException("Hadoop class is not available");
    }
  }

  private static void requireClz(Object arg, String requiredClzName) {
    String argClzName = arg.getClass().getName();
    if (!requiredClzName.equals(argClzName)) {
      throw new IllegalArgumentException("Require " + requiredClzName + " but got " + argClzName);
    }
  }

  public static class UserGroupInformation {

    private static Class<?> ugiClz;
    private static Method getCurrentUserMethod;
    private static Method getTokensMethod;
    private static Field subjectField;

    static {
      if (hadoopClzAvailable) {
        try {
          ugiClz = Class.forName(UGI_CLASS);
          getCurrentUserMethod = ugiClz.getDeclaredMethod("getCurrentUser");
          getTokensMethod = ugiClz.getDeclaredMethod("getTokens");
          subjectField = ugiClz.getDeclaredField("subject");
          subjectField.setAccessible(true);
        } catch (NoClassDefFoundError | ReflectiveOperationException e) {
          throw new RuntimeException(e);
        }
      }
    }

    /**
     * Forward to UserGroupInformation#getCurrentUser()
     *
     * @return UserGroupInformation
     */
    public static Object getCurrentUser() {
      ensureHadoopClzAvailable();
      try {
        return getCurrentUserMethod.invoke(null);
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
    }

    public static Subject getCurrentSubject() {
      return getSubject(getCurrentUser());
    }

    public static Subject getSubject(Object ugi) {
      ensureHadoopClzAvailable();
      requireClz(ugi, UGI_CLASS);
      try {
        return (Subject) subjectField.get(ugi);
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Forward to UserGroupInformation#getTokens()
     *
     * @param ugi UserGroupInformation
     * @return Collection<getTokens>
     */
    @SuppressWarnings("rawtypes")
    public static Collection getTokens(Object ugi) {
      ensureHadoopClzAvailable();
      requireClz(ugi, UGI_CLASS);
      try {
        return (Collection) getTokensMethod.invoke(ugi);
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class Token {
    private static Class<?> tokenClz;
    private static Constructor<?> tokenConstructor;
    private static Method getIdentifierMethod;
    private static Method getPasswordMethod;
    private static Method encodeToUrlStringMethod;
    private static Method decodeFromUrlStringMethod;

    static {
      if (hadoopClzAvailable) {
        try {
          tokenClz = Class.forName(TOKEN_CLASS);
          tokenConstructor = tokenClz.getConstructor();
          getIdentifierMethod = tokenClz.getDeclaredMethod("getIdentifier");
          getPasswordMethod = tokenClz.getDeclaredMethod("getPassword");
          encodeToUrlStringMethod = tokenClz.getDeclaredMethod("encodeToUrlString");
          decodeFromUrlStringMethod =
              tokenClz.getDeclaredMethod("decodeFromUrlString", String.class);
        } catch (NoClassDefFoundError | ReflectiveOperationException e) {
          throw new RuntimeException(e);
        }
      }
    }

    /**
     * Forward to new Token()
     *
     * @return org.apache.hadoop.security.token.Token
     */
    public static Object newInstance() {
      ensureHadoopClzAvailable();
      try {
        return tokenConstructor.newInstance();
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Forward to Token#encodeToUrlString
     *
     * @param token org.apache.hadoop.security.token.Token
     * @return encodeToUrlString
     */
    public static String encodeToUrlString(Object token) {
      ensureHadoopClzAvailable();
      requireClz(token, TOKEN_CLASS);
      try {
        return (String) encodeToUrlStringMethod.invoke(token);
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
    }

    public static void decodeFromUrlString(Object token, String newValue) {
      ensureHadoopClzAvailable();
      requireClz(token, TOKEN_CLASS);
      try {
        decodeFromUrlStringMethod.invoke(token, newValue);
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
    }

    public static byte[] getIdentifier(Object token) {
      ensureHadoopClzAvailable();
      requireClz(token, TOKEN_CLASS);
      try {
        return (byte[]) getIdentifierMethod.invoke(token);
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
    }

    public static byte[] getPassword(Object token) {
      ensureHadoopClzAvailable();
      requireClz(token, TOKEN_CLASS);
      try {
        return (byte[]) getPasswordMethod.invoke(token);
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class AbstractDelegationTokenSelector {

    private static Class<?> abstractDelegationTokenSelectorClz;
    private static Constructor<?> abstractDelegationTokenSelectorConstructor;
    private static Method selectTokenMethod;

    static {
      if (hadoopClzAvailable) {
        try {
          abstractDelegationTokenSelectorClz =
              Class.forName(ABSTRACT_DELEGATION_TOKEN_SELECTOR_CLASS);
          abstractDelegationTokenSelectorConstructor =
              abstractDelegationTokenSelectorClz.getConstructor();
          abstractDelegationTokenSelectorConstructor.setAccessible(true);
          selectTokenMethod =
              abstractDelegationTokenSelectorClz.getDeclaredMethod(
                  "selectToken", Text.textClz, Collection.class);
        } catch (NoClassDefFoundError | ReflectiveOperationException e) {
          throw new RuntimeException(e);
        }
      }
    }

    /**
     * Forward to new DelegationTokenSelector()
     *
     * @return org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSelector
     */
    public static Object newInstance(Object text) {
      ensureHadoopClzAvailable();
      requireClz(text, TEXT_CLASS);
      try {
        return abstractDelegationTokenSelectorConstructor.newInstance(text);
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Forward to AbstractDelegationTokenSelector#selectToken( Text service, Collection<Token<?
     * extends TokenIdentifier>> tokens)
     *
     * @return org.apache.hadoop.security.token
     */
    @SuppressWarnings("rawtypes")
    public static Object selectToken(
        Object delegationTokenSelector, Object service, Collection tokens) {
      try {
        return selectTokenMethod.invoke(delegationTokenSelector, service, tokens);
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class Text {
    private static Class<?> textClz;
    private static Constructor<?> textConstructor;
    private static Constructor<?> textStringConstructor;

    static {
      if (hadoopClzAvailable) {
        try {
          textClz = Class.forName(TEXT_CLASS);
          textConstructor = textClz.getConstructor();
          textStringConstructor = textClz.getConstructor(String.class);
        } catch (NoClassDefFoundError | ReflectiveOperationException e) {
          throw new RuntimeException(e);
        }
      }
    }

    /**
     * Forward to new Text()
     *
     * @return org.apache.hadoop.io.Text
     */
    public static Object newInstance() {
      ensureHadoopClzAvailable();
      try {
        return textConstructor.newInstance();
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Forward to new Text(String text)
     *
     * @return org.apache.hadoop.io.Text
     */
    public static Object newInstance(String text) {
      ensureHadoopClzAvailable();
      try {
        return textStringConstructor.newInstance(text);
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Get the string form of the token given a token signature. The signature is used as the value of
   * the "service" field in the token for lookup. Ref: AbstractDelegationTokenSelector in Hadoop. If
   * there exists such a token in the token cache (credential store) of the job, the lookup returns
   * that. This is relevant only when running against a "secure" hadoop release The method gets hold
   * of the tokens if they are set up by hadoop - this should happen on the map/reduce tasks if the
   * client added the tokens into hadoop's credential store in the front end during job submission.
   * The method will select the hive delegation token among the set of tokens and return the string
   * form of it
   */
  public static String getTokenStrForm(String tokenSignature) {
    Object ugi = HadoopShim.UserGroupInformation.getCurrentUser();
    Object kind = HadoopShim.Text.newInstance(KYUUBI_DELEGATION_TOKEN_KIND);
    Object tokenSelector = AbstractDelegationTokenSelector.newInstance(kind);
    Object service =
        tokenSignature == null
            ? HadoopShim.Text.newInstance()
            : HadoopShim.Text.newInstance(tokenSignature);
    Object token =
        AbstractDelegationTokenSelector.selectToken(
            tokenSelector, service, HadoopShim.UserGroupInformation.getTokens(ugi));
    return token != null ? Token.encodeToUrlString(token) : null;
  }
}
