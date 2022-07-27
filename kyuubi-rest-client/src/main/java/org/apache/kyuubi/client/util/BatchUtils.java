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
  /** The batch has not been submitted to resource manager yet. */
  public static String PENDING_STATE = "PENDING";

  /** The batch has been submitted to resource manager and is running. */
  public static String RUNNING_STATE = "RUNNING";

  /** The batch has been finished successfully. */
  public static String FINISHED_STATE = "FINISHED";

  /** The batch met some issue and failed. */
  public static String ERROR_STATE = "ERROR";

  /** The batch was closed by `DELETE /batches/${batchId}` api. */
  public static String CANCELED_STATE = "CANCELED";

  public static List<String> terminalBatchStates =
      Arrays.asList(FINISHED_STATE, ERROR_STATE, CANCELED_STATE);

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
