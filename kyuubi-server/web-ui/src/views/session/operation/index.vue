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
  <el-card :body-style="{ padding: '10px 14px' }">
    <header>
      <el-breadcrumb v-if="sessionId && sessionId !== 'all'" separator="/">
        <el-breadcrumb-item :to="{ path: '/session/operation/all' }"
          >Operation</el-breadcrumb-item
        >
        <el-breadcrumb-item>{{ sessionId }}</el-breadcrumb-item>
      </el-breadcrumb>
      <span v-else></span>
      <el-space class="search-box">
        <el-input v-model="searchParam" placeholder="Please input" />
        <el-button type="primary" icon="Search" />
      </el-space>
    </header>
  </el-card>
  <el-card class="table-container">
    <el-table v-loading="loading" :data="tableData" style="width: 100%">
      <el-table-column prop="sessionId" :label="$t('session_id')" width="160" />
      <el-table-column
        prop="statementId"
        :label="$t('operation_id')"
        width="160"
      />
      <el-table-column prop="operationType" :label="$t('type')" width="160" />
      <el-table-column prop="state" :label="$t('state')" width="160" />
      <el-table-column :label="$t('start_time')" width="200">
        <template #default="scope">
          {{
            scope.row.startTime != null && scope.row.startTime > -1
              ? format(scope.row.startTime, 'yyyy-MM-dd HH:mm:ss')
              : '-'
          }}
        </template>
      </el-table-column>
      <el-table-column :label="$t('finish_time')" width="200">
        <template #default="scope">
          {{
            scope.row.completeTime != null && scope.row.completeTime > -1
              ? format(scope.row.completeTime, 'yyyy-MM-dd HH:mm:ss')
              : '-'
          }}
        </template>
      </el-table-column>
      <el-table-column :label="$t('duration')" width="200">
        <template #default="scope">{{
          scope.row.startTime != null &&
          scope.row.completeTime != null &&
          scope.row.startTime > -1 &&
          scope.row.completeTime > -1
            ? secondTransfer(
                (scope.row.completeTime - scope.row.startTime) / 1000
              )
            : '-'
        }}</template>
      </el-table-column>
      <el-table-column fixed="right" :label="$t('operation')" width="120">
        <template #default="scope">
          <el-space wrap>
            <el-popconfirm
              :title="$t('cancel_confirm')"
              @confirm="handleOperate(scope.row.statementId, 'CANCEL')"
            >
              <template #reference>
                <span>
                  <el-tooltip
                    effect="dark"
                    :content="$t('cancel')"
                    placement="top"
                  >
                    <template #default>
                      <el-button type="danger" icon="Remove" circle />
                    </template>
                  </el-tooltip>
                </span>
              </template>
            </el-popconfirm>
            <el-popconfirm
              :title="$t('close_confirm')"
              @confirm="handleOperate(scope.row.statementId, 'CLOSE')"
            >
              <template #reference>
                <span>
                  <el-tooltip
                    effect="dark"
                    :content="$t('close')"
                    placement="top"
                  >
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
  import { ref, watch } from 'vue'
  import { useRoute } from 'vue-router'
  import { getAllOperations, cancelOperation } from '@/api/session'
  import { format } from 'date-fns'
  import { useI18n } from 'vue-i18n'
  import { ElMessage } from 'element-plus'
  import { secondTransfer } from '@/utils'

  const searchParam = ref()
  const tableData: any = ref([])
  const loading = ref(false)
  const sessionId = ref()
  const route = useRoute()
  const { t } = useI18n()
  sessionId.value = route.params.sessionId

  const handleOperate = (
    operationId: string,
    operation: 'CANCEL' | 'CLOSE'
  ) => {
    cancelOperation(operationId, { action: operation }).then(() => {
      // need add delete success or failed logic after api support
      ElMessage({
        message: t(`${operation.toLowerCase()}_success`),
        type: 'success'
      })
      getOperationList()
    })
  }

  const getOperationList = () => {
    if (sessionId.value !== 'all') {
      loading.value = true
      getAllOperations(sessionId.value as string)
        .then((res: any) => {
          tableData.value = res || []
        })
        .finally(() => {
          loading.value = false
        })
    } else {
      // mock api: get all opertaions
      tableData.value = []
    }
  }
  getOperationList()

  watch(
    () => route.path,
    (toPath: string) => {
      const path = '/session/operation/'
      if (toPath.indexOf(path) > -1) {
        sessionId.value = toPath.replaceAll(path, '')
        getOperationList()
      }
    }
  )
</script>
<style lang="scss" scoped>
  header {
    display: flex;
    justify-content: space-between;
    .el-breadcrumb {
      line-height: 32px;
    }
  }
</style>
