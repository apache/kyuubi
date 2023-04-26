<!--
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
-->

<template>
  <el-card class="table-container">
    <el-table v-loading="loading" :data="tableData" style="width: 100%">
      <el-table-column prop="sessionUser" :label="$t('user')" width="160" />
      <el-table-column
        prop="identifier"
        :label="$t('operation_id')"
        width="300" />
      <el-table-column prop="statement" :label="$t('statement')" width="160" />
      <el-table-column prop="state" :label="$t('state')" width="160" />
      <el-table-column :label="$t('start_time')" width="160">
        <template #default="scope">
          {{
            scope.row.startTime != null && scope.row.startTime > 0
              ? format(scope.row.startTime, 'yyyy-MM-dd HH:mm:ss')
              : '-'
          }}
        </template>
      </el-table-column>
      <el-table-column :label="$t('complete_time')" width="160">
        <template #default="scope">
          {{
            scope.row.completeTime != null && scope.row.completeTime > 0
              ? format(scope.row.completeTime, 'yyyy-MM-dd HH:mm:ss')
              : '-'
          }}
        </template>
      </el-table-column>
      <el-table-column :label="$t('duration')" width="140">
        <template #default="scope">{{
          scope.row.startTime != null &&
          scope.row.completeTime != null &&
          scope.row.startTime > 0 &&
          scope.row.completeTime > 0
            ? millTransfer(scope.row.completeTime - scope.row.startTime)
            : '-'
        }}</template>
      </el-table-column>
      <el-table-column fixed="right" :label="$t('operation.text')" width="110">
        <template #default="scope">
          <el-space wrap>
            <el-popconfirm
              v-if="!isTerminalState(scope.row.state)"
              :title="$t('operation.cancel_confirm')"
              @confirm="handleOperate(scope.row.identifier, 'CANCEL')">
              <template #reference>
                <span>
                  <el-tooltip
                    effect="dark"
                    :content="$t('operation.cancel')"
                    placement="top">
                    <template #default>
                      <el-button type="danger" icon="Remove" circle />
                    </template>
                  </el-tooltip>
                </span>
              </template>
            </el-popconfirm>
            <el-popconfirm
              :title="$t('operation.close_confirm')"
              @confirm="handleOperate(scope.row.identifier, 'CLOSE')">
              <template #reference>
                <span>
                  <el-tooltip
                    effect="dark"
                    :content="$t('operation.close')"
                    placement="top">
                    <template #default>
                      <el-button type="danger" icon="CircleClose" circle />
                    </template>
                  </el-tooltip>
                </span>
              </template>
            </el-popconfirm>
          </el-space>
        </template>
      </el-table-column>
    </el-table>
  </el-card>
</template>

<script lang="ts" setup>
  import { getAllOperations, actionOnOperation } from '@/api/operation'
  import { millTransfer } from '@/utils/unit'
  import { format } from 'date-fns'
  import { useI18n } from 'vue-i18n'
  import { ElMessage } from 'element-plus'
  import { useTable } from '@/views/common/use-table'

  const { t } = useI18n()
  const { tableData, loading, getList: _getList } = useTable()
  const handleOperate = (operationId: string, action: 'CANCEL' | 'CLOSE') => {
    actionOnOperation(operationId, { action: action }).then(() => {
      // TODO add delete success or failed logic after api support
      ElMessage({
        message: t(`${action.toLowerCase()}_succeeded`, {
          operationId: operationId
        }),
        type: 'success'
      })
      getList()
    })
  }
  const getList = () => {
    _getList(getAllOperations)
  }

  const terminalStates = new Set([
    'FINISHED_STATE',
    'CLOSED_STATE',
    'CANCELED_STATE',
    'TIMEOUT_STATE',
    'ERROR_STATE'
  ])

  function isTerminalState(state: string): Boolean {
    return terminalStates.has(state)
  }
  getList()
</script>
<style lang="scss" scoped>
  header {
    display: flex;
    justify-content: flex-end;
  }
</style>
