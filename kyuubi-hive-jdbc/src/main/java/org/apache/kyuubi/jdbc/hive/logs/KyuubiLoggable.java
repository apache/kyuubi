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

package org.apache.kyuubi.jdbc.hive.logs;

import java.sql.SQLException;
import java.util.List;
import org.apache.kyuubi.jdbc.hive.ClosedOrCancelledException;

public interface KyuubiLoggable {
  /**
   * Check whether the execution might be producing more logs to be fetched.
   *
   * @return true if the execution might be producing more logs. It does not indicate if last log
   *     lines have been fetched by getQueryLog.
   */
  boolean hasMoreLogs();

  /**
   * Get the execution logs. This method gets the incremental logs during SQL execution, and uses
   * fetchSize holden by HiveStatement object.
   *
   * @return a list of logs. It can be empty if there are no new logs to be retrieved at that time.
   * @throws SQLException
   * @throws ClosedOrCancelledException if the execution has been cancelled or closed
   */
  List<String> getExecLog() throws SQLException, ClosedOrCancelledException;
}
