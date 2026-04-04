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
  test: '测试',
  user: '用户',
  client_ip: '客户端地址',
  server_ip: '服务端地址',
  kyuubi_instance: '服务端实例',
  session_id: 'Session ID',
  operation_id: 'Operation ID',
  create_time: '创建时间',
  start_time: '开始时间',
  complete_time: '完成时间',
  state: '状态',
  duration: '运行时间',
  statement: 'Statement',
  engine_address: 'Engine 地址',
  engine_id: 'Engine ID',
  engine_type: 'Engine 类型',
  share_level: '共享级别',
  version: '版本',
  engine_ui: 'Engine UI',
  failure_reason: '失败原因',
  session_properties: 'Session 参数',
  no_data: '无数据',
  no_log: '无日志',
  run_sql_tips: '请运行SQL获取结果',
  result: '结果',
  log: '日志',
  operation: {
    text: '操作',
    delete_confirm: '确认删除',
    close_confirm: '确认关闭',
    cancel_confirm: '确认取消',
    close: '关闭',
    cancel: '取消',
    delete: '删除',
    run: '运行'
  },
  data_agent: {
    title: 'Data Agent',
    welcome_desc:
      '用自然语言提问，智能体将自动探索数据库结构、编写 SQL 查询并分析结果。',
    connection: '连接配置',
    jdbc_url: 'JDBC URL',
    server_default: '使用服务端默认',
    jdbc_placeholder: '留空则使用服务端默认配置',
    try_asking: '试试这些问题',
    quick_tables: '有哪些可用的表？',
    quick_schema: '描述一下某张表的字段',
    quick_records: '随便看几条示例数据',
    auto_approve: '自动审批',
    normal: '普通',
    strict: '严格',
    approval_tooltip: '控制工具调用是否需要你的审批才能执行',
    stop: '停止',
    new_chat: '新对话',
    change_jdbc: '点击"新对话"以更换 JDBC URL',
    starting_engine: '正在启动 Data Agent 引擎...',
    waiting_response: '等待响应中...',
    input_placeholder: '输入关于数据的问题...',
    input_hint_send: '发送',
    input_hint_newline: '换行',
    session_expired: '会话已过期，请点击"新对话"开始新的对话。',
    session_start_failed: '启动会话失败：{message}',
    session_close_failed: '关闭上一个会话失败，将重新开始。',
    malformed_response: '收到服务端格式异常的响应',
    stream_error: '流式传输错误',
    unknown_error: '未知错误',
    approval_failed: '审批失败：{message}',
    approval_required: '需要审批',
    arguments: '参数',
    result: '结果',
    approve: '批准',
    deny: '拒绝',
    approved: '已批准',
    denied: '已拒绝',
    running: '运行中...',
    done: '完成',
    error: '错误',
    generating: '生成中...',
    copied: '已复制',
    copy_failed: '复制失败',
    copy: '复制',
    history: '最近使用'
  },
  message: {
    delete_succeeded: '删除 {name} 成功',
    delete_failed: '删除 {name} 失败',
    close_succeeded: '关闭 {name} 成功',
    close_failed: '关闭 {name} 失败',
    cancel_succeeded: '取消 {name} 成功',
    cancel_failed: '取消 {name} 失败',
    run_sql_failed: '运行SQL失败',
    get_sql_log_failed: '获取SQL日志失败',
    get_sql_result_failed: '获取SQL结果失败',
    get_sql_metadata_failed: '获取SQL元数据失败'
  }
}
