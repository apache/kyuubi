/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.launcher;

import java.util.*;

import static org.apache.spark.launcher.CommandBuilderUtils.*;

public class KyuubiMain {
    /**
     * Usage: Main [class] [class args]
     */
    public static void main(String[] argsArray) throws Exception {
        checkArgument(argsArray.length > 0, "Not enough arguments: missing class name.");

        List<String> args = new ArrayList<>(Arrays.asList(argsArray));
        String className = args.remove(0);

        AbstractCommandBuilder builder;
        try {
            builder = new SparkSubmitCommandBuilder(args);
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            System.err.println();

            MainClassOptionParser parser = new MainClassOptionParser();
            try {
                parser.parse(args);
            } catch (Exception ignored) {
                // Ignore parsing exceptions.
            }

            List<String> help = new ArrayList<>();
            if (parser.className != null) {
                help.add(parser.CLASS);
                help.add(parser.className);
            }
            help.add(parser.USAGE_ERROR);
            builder = new SparkSubmitCommandBuilder(help);
        }

        Map<String, String> env = new HashMap<>();
        List<String> cmd = builder.buildCommand(env);
        System.err.println("Spark Command: " + join(" ", cmd));
        System.err.println("========================================");

        // In bash, use NULL as the arg separator since it cannot be used in an argument.
        List<String> bashCmd = prepareBashCommand(cmd, env);
        for (String c : bashCmd) {
            System.out.print(c);
            System.out.print('\0');
        }
    }

    /**
     * Prepare the command for execution from a bash script. The final command will have commands to
     * set up any needed environment variables needed by the child process.
     */
    private static List<String> prepareBashCommand(List<String> cmd, Map<String, String> childEnv) {
        if (childEnv.isEmpty()) {
            return cmd;
        }

        List<String> newCmd = new ArrayList<>();
        newCmd.add("env");

        for (Map.Entry<String, String> e : childEnv.entrySet()) {
            newCmd.add(String.format("%s=%s", e.getKey(), e.getValue()));
        }
        newCmd.addAll(cmd);
        return newCmd;
    }

    /**
     * A parser used when command line parsing fails for spark-submit. It's used as a best-effort
     * at trying to identify the class the user wanted to invoke, since that may require special
     * usage strings (handled by SparkSubmitArguments).
     */
    private static class MainClassOptionParser extends SparkSubmitOptionParser {

        String className;

        @Override
        protected boolean handle(String opt, String value) {
            if (CLASS.equals(opt)) {
                className = value;
            }
            return false;
        }

        @Override
        protected boolean handleUnknown(String opt) {
            return false;
        }

        @Override
        protected void handleExtraArgs(List<String> extra) {

        }

    }
}
