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

export default {
  test: 'test',
  user: 'User',
  client_ip: 'Client IP',
  server_ip: 'Server IP',
  kyuubi_instance: 'Kyuubi Instance',
  session_id: 'Session ID',
  operation_id: 'Operation ID',
  create_time: 'Create Time',
  start_time: 'State Time',
  complete_time: 'Completed Time',
  state: 'State',
  duration: 'Duration',
  statement: 'Statement',
  engine_address: 'Engine Address',
  engine_id: 'Engine ID',
  engine_type: 'Engine Type',
  share_level: 'Share Level',
  version: 'Version',
  engine_ui: 'Engine UI',
  failure_reason: 'Failure Reason',
  session_properties: 'Session Properties',
  no_data: 'No data',
  no_log: 'No log',
  run_sql_tips: 'Run a SQL to get result',
  result: 'Result',
  log: 'Log',
  operation: {
    text: 'Operation',
    delete_confirm: 'Delete Confirm',
    close_confirm: 'Close Confirm',
    cancel_confirm: 'Cancel Confirm',
    close: 'Close',
    cancel: 'Cancel',
    delete: 'Delete',
    run: 'Run'
  },
  data_agent: {
    title: 'Data Agent',
    welcome_desc:
      'Ask questions about your data in natural language. The agent will explore schemas, write SQL queries, and analyze results.',
    connection: 'Connection',
    jdbc_url: 'JDBC URL',
    server_default: 'Server default',
    jdbc_placeholder: 'Leave empty to use server default',
    try_asking: 'Try asking',
    quick_tables: 'What tables are available?',
    quick_schema: 'Describe the columns of a table',
    quick_records: 'Show me a few sample rows',
    auto_approve: 'Auto Approve',
    normal: 'Normal',
    strict: 'Strict',
    approval_tooltip:
      'Controls whether tool calls require your approval before execution',
    stop: 'Stop',
    new_chat: 'New Chat',
    change_jdbc: 'Click "New Chat" to change JDBC URL',
    starting_engine: 'Starting Data Agent engine...',
    waiting_response: 'Waiting for response...',
    input_placeholder: 'Ask a question about your data...',
    input_hint_send: 'send',
    input_hint_newline: 'new line',
    session_expired:
      'Session has expired. Please click "New Chat" to start a new conversation.',
    session_start_failed: 'Failed to start session: {message}',
    session_close_failed: 'Failed to close previous session, starting fresh.',
    malformed_response: 'Received malformed response from server',
    stream_error: 'Stream error',
    unknown_error: 'Unknown error',
    approval_failed: 'Approval failed: {message}',
    approval_required: 'Approval Required',
    arguments: 'Arguments',
    result: 'Result',
    approve: 'Approve',
    deny: 'Deny',
    approved: 'Approved',
    denied: 'Denied',
    running: 'Running...',
    done: 'Done',
    error: 'Error',
    generating: 'Generating...',
    copied: 'Copied',
    copy_failed: 'Copy failed',
    copy: 'Copy',
    history: 'Recent'
  },
  message: {
    delete_succeeded: 'Delete {name} Succeeded',
    delete_failed: 'Delete {name} Failed',
    close_succeeded: 'Close {name} Succeeded',
    close_failed: 'Close {name} Failed',
    cancel_succeeded: 'Cancel {name} Succeeded',
    cancel_failed: 'Cancel {name} Failed',
    run_sql_failed: 'Run SQL Failed',
    get_sql_log_failed: 'Get SQL Log Failed',
    get_sql_result_failed: 'Get SQL Result Failed',
    get_sql_metadata_failed: 'Get SQL Metadata Failed'
  }
}
