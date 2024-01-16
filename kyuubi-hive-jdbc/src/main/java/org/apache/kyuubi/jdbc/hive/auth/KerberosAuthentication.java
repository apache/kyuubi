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

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.isReadable;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KerberosAuthentication {
  private static final Logger LOG = LoggerFactory.getLogger(KerberosAuthentication.class);
  private static final String KERBEROS_LOGIN_MODULE =
      "com.sun.security.auth.module.Krb5LoginModule";

  private KerberosPrincipal principal = null;
  private final Configuration configuration;

  KerberosAuthentication(String ticketCache) {
    this.configuration = createLoginFromTgtCacheConfiguration(ticketCache);
  }

  KerberosAuthentication(String principal, String keytabLocation) {
    requireNonNull(principal, "principal is null");
    requireNonNull(keytabLocation, "keytabLocation is null");
    Path keytabPath = Paths.get(keytabLocation);
    checkArgument(exists(keytabPath), "keytab does not exist: %s", keytabLocation);
    checkArgument(isReadable(keytabPath), "keytab is not readable: %s", keytabLocation);
    this.principal = createKerberosPrincipal(principal);
    this.configuration =
        createLoginFromKeytabConfiguration(this.principal.getName(), keytabLocation);
  }

  public Subject getSubject() {
    Subject subject =
        principal == null
            ? null
            : new Subject(false, ImmutableSet.of(principal), emptySet(), emptySet());
    try {
      LoginContext loginContext = new LoginContext("", subject, null, configuration);
      loginContext.login();
      return loginContext.getSubject();
    } catch (LoginException e) {
      throw new RuntimeException(e);
    }
  }

  public void attemptLogin(Subject subject) {
    try {
      LoginContext loginContext = new LoginContext("", subject, null, configuration);
      loginContext.login();
    } catch (LoginException e) {
      throw new RuntimeException(e);
    }
  }

  private static KerberosPrincipal createKerberosPrincipal(String principal) {
    try {
      return new KerberosPrincipal(
          KerberosUtils.canonicalClientPrincipal(
              principal, InetAddress.getLocalHost().getCanonicalHostName()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Configuration createLoginFromTgtCacheConfiguration(String ticketCache) {
    ImmutableMap.Builder<String, String> optionsBuilder =
        ImmutableMap.<String, String>builder()
            .put("useTicketCache", "true")
            .put("renewTGT", "true");

    if (StringUtils.isBlank(ticketCache)) {
      ticketCache = System.getenv("KRB5CCNAME");
    }
    if (StringUtils.isNotBlank(ticketCache)) {
      if (!Files.exists(Paths.get(ticketCache))) {
        LOG.warn("TicketCache {} does not exist", ticketCache);
      }
      optionsBuilder.put("ticketCache", ticketCache);
    }
    return createConfiguration(optionsBuilder);
  }

  private static Configuration createLoginFromKeytabConfiguration(
      String principal, String keytabLocation) {
    ImmutableMap.Builder<String, String> optionsBuilder =
        ImmutableMap.<String, String>builder()
            .put("useKeyTab", "true")
            .put("storeKey", "true")
            .put("refreshKrb5Config", "true")
            .put("principal", principal)
            .put("keyTab", keytabLocation);

    return createConfiguration(optionsBuilder);
  }

  private static Configuration createConfiguration(
      ImmutableMap.Builder<String, String> optionsBuilder) {

    if (LOG.isDebugEnabled()) {
      optionsBuilder.put("debug", "true");
    }

    Map<String, String> options = optionsBuilder.put("doNotPrompt", "true").build();

    return new Configuration() {
      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        return new AppConfigurationEntry[] {
          new AppConfigurationEntry(
              KERBEROS_LOGIN_MODULE, AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options)
        };
      }
    };
  }
}
