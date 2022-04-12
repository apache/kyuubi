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
package org.apache.kyuubi.engine.hive.deploy;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HiveSubmitOptionParser {

  protected final String DEPLOY_MODE = "--deploy-mode";
  protected final String MASTER = "--master";
  protected final String FILES = "--files";
  protected final String PROXY_USER = "--proxy-user";

  protected final String QUEUE = "--queue";
  protected final String PRINCIPAL = "--principal";
  protected final String KEYTAB = "--keytab";

  protected final String VERBOSE = "--verbose";

  protected final String CLASSPATH = "--classpath";

  protected final String CONF = "--conf";

  final String[][] opts = {
    {DEPLOY_MODE},
    {FILES},
    {KEYTAB},
    {MASTER},
    {PRINCIPAL},
    {PROXY_USER},
    {QUEUE},
    {CLASSPATH},
    {CONF}
  };

  final String[][] switches = {{VERBOSE, "-v"}};

  protected final void parse(List<String> args) {
    Pattern eqSeparatedOpt = Pattern.compile("(--[^=]+)=(.+)");

    int idx = 0;
    for (idx = 0; idx < args.size(); idx++) {
      String arg = args.get(idx);
      String value = null;

      Matcher m = eqSeparatedOpt.matcher(arg);
      if (m.matches()) {
        arg = m.group(1);
        value = m.group(2);
      }

      // Look for options with a value.
      String name = findCliOption(arg, opts);
      if (name != null) {
        if (value == null) {
          if (idx == args.size() - 1) {
            throw new IllegalArgumentException(
                String.format("Missing argument for option '%s'.", arg));
          }
          idx++;
          value = args.get(idx);
        }
        if (!handle(name, value)) {
          break;
        }
        continue;
      }

      // Look for a switch.
      name = findCliOption(arg, switches);
      if (name != null) {
        if (!handle(name, null)) {
          break;
        }
        continue;
      }

      if (!handleUnknown(arg)) {
        break;
      }
    }

    if (idx < args.size()) {
      idx++;
    }
    handleExtraArgs(args.subList(idx, args.size()));
  }

  protected boolean handle(String opt, String value) {
    throw new UnsupportedOperationException();
  }

  protected boolean handleUnknown(String opt) {
    throw new UnsupportedOperationException();
  }

  protected void handleExtraArgs(List<String> extra) {
    throw new UnsupportedOperationException();
  }

  private String findCliOption(String name, String[][] available) {
    for (String[] candidates : available) {
      for (String candidate : candidates) {
        if (candidate.equals(name)) {
          return candidates[0];
        }
      }
    }
    return null;
  }
}
