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

package org.apache.kyuubi.ctl;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class KyuubiCtlOptionParser {
  protected final String ZK_ADDRESS = "--zkAddress";
  protected final String NAMESPACE = "--namespace";
  protected final String USER = "--user";
  protected final String HOST = "--host";
  protected final String PORT = "--port";
  protected final String VERSION = "--version";
  protected final String CREATE = "create";
  protected final String GET = "get";
  protected final String DELETE = "delete";
  protected final String LIST = "list";
  protected final String SERVER = "server";
  protected final String ENGINE = "engine";

  // Options that do not take arguments.
  protected final String HELP = "--help";
  protected final String VERBOSE = "--verbose";

  final String[][] opts = {
    { ZK_ADDRESS, "-zk" },
    { NAMESPACE, "-ns" },
    { USER, "-u" },
    { HOST, "-h" },
    { PORT, "-p" },
    { VERSION, "-V" },
  };

  /**
   * List of switches (command line options that do not take parameters) recognized by
   * kyuubi-ctl.
   */
  final String[][] switches = {
    { HELP, "-I" },
    { VERBOSE, "-v" },
  };

  /**
   * Parse action type and service type.
   *
   * @return offset of remaining arguments.
   */
  protected int parseActionAndService(List<String> args) {
    throw new UnsupportedOperationException();
  }

  /**
   * Parse a list of kyuubi-ctl command line options.
   * <p>
   * See KyuubiCtlArguments.scala for a more formal description of available options.
   *
   * @throws IllegalArgumentException If an error is found during parsing.
   */
  protected final void parse(List<String> args) {
    Pattern eqSeparatedOpt = Pattern.compile("(--[^=]+)=(.+)");

    int idx = parseActionAndService(args);
    for (; idx < args.size(); idx++) {
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

      handleUnknown(arg);
    }
  }

  /**
   * Callback for when an option with an argument is parsed.
   *
   * @param opt The long name of the cli option (might differ from actual command line).
   * @param value The value. This will be <i>null</i> if the option does not take a value.
   * @return Whether to continue parsing the argument list.
   */
  protected boolean handle(String opt, String value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Callback for when an unrecognized option is parsed.
   *
   * @param opt Unrecognized option from the command line.
   * @return Whether to continue parsing the argument list.
   */
  protected boolean handleUnknown(String opt) {
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

  protected String findSwitches(String name) {
    return findCliOption(name, switches);
  }
}
