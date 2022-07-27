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

package org.apache.kyuubi.client.util;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public final class BatchUtils {
    /**
     * The lifecycle of batch state is:
     *
     *                        /  ERROR
     * PENDING  ->  RUNNING  ->  FINISHED
     *                        \  CANCELED
     *
     */
    static String PENDING_STATE = "PENDING";
    static String RUNNING_STATE = "RUNNING";
    static String FINISHED_STATE = "FINISHED";
    static String ERROR_STATE = "ERROR";
    static String CANCELED_STATE = "CANCELED";
    static List<String> terminalBatchStates = Arrays.asList(FINISHED_STATE, ERROR_STATE, CANCELED_STATE);

    public static boolean isPendingState(String state) {
        return PENDING_STATE.equalsIgnoreCase(state);
    }

    public static boolean isRunningState(String state) {
        return RUNNING_STATE.equalsIgnoreCase(state);
    }

    public static boolean isFinishedState(String state) {
        return FINISHED_STATE.equalsIgnoreCase(state);
    }

    public static boolean isTerminalState(String state) {
        return state != null && terminalBatchStates.contains(state.toUpperCase(Locale.ROOT));
    }
}
