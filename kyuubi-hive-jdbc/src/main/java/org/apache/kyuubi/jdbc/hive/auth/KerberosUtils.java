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

import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

import java.util.Set;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;

public final class KerberosUtils {

  private static final String HOSTNAME_PATTERN = "_HOST";

  private static final float TICKET_RENEW_WINDOW = 0.80f;

  public static String[] splitPrincipal(String principal) {
    return principal.split("[/@]");
  }

  public static String canonicalPrincipal(String principal, String hostname) {
    String[] names = splitPrincipal(principal);
    if (names.length != 3) {
      throw new IllegalArgumentException("Kerberos principal should have 3 parts: " + principal);
    }
    if (!names[1].equals(HOSTNAME_PATTERN)) {
      return principal;
    }
    return format("%s/%s@%s", names[0], hostname.toLowerCase(ENGLISH), names[2]);
  }

  public static String canonicalClientPrincipal(String principal, String hostname) {
    String[] components = splitPrincipal(principal);
    if (components.length != 3 || !components[1].equals(HOSTNAME_PATTERN)) {
      return principal;
    } else {
      return canonicalPrincipal(principal, hostname);
    }
  }

  public static KerberosTicket getTgt(Subject subject) {
    Set<KerberosTicket> tickets = subject.getPrivateCredentials(KerberosTicket.class);
    for (KerberosTicket ticket : tickets) {
      if (isOriginalTgt(ticket)) {
        return ticket;
      }
    }
    throw new IllegalArgumentException("kerberos ticket not found in " + subject);
  }

  public static long getTgtRefreshTime(KerberosTicket ticket) {
    long start = ticket.getStartTime().getTime();
    long end = ticket.getEndTime().getTime();
    return start + (long) ((end - start) * TICKET_RENEW_WINDOW);
  }

  /**
   * Check whether the server principal is the TGS's principal
   *
   * @param ticket the original TGT (the ticket that is obtained when a kinit is done)
   * @return true or false
   */
  public static boolean isOriginalTgt(KerberosTicket ticket) {
    return isTgsPrincipal(ticket.getServer());
  }

  /**
   * TGS(Ticket Granting Server) must have the server principal of the form "krbtgt/FOO@FOO".
   *
   * @return true or false
   */
  private static boolean isTgsPrincipal(KerberosPrincipal principal) {
    if (principal == null) {
      return false;
    }
    return principal
        .getName()
        .equals("krbtgt/" + principal.getRealm() + "@" + principal.getRealm());
  }
}
