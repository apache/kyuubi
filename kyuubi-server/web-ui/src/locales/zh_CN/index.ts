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
