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
      <el-space class="search-box">
        <el-input
          v-model="searchParam.sessionHandle"
          :placeholder="$t('session_id')"
          style="width: 320px"
          @keyup.enter="getList"
        />
        <el-select
          v-model="searchParam.operationType"
          :placeholder="$t('operation_type')"
          clearable
          style="width: 210px"
          @change="getList"
        >
          <el-option
            v-for="item in [
              'EXECUTE_STATEMENT',
              'GET_TYPE_INFO',
              'GET_CATALOGS',
              'GET_SCHEMAS',
              'GET_TABLES',
              'GET_TABLE_TYPES',
              'GET_COLUMNS',
              'GET_FUNCTIONS',
              'UNKNOWN'
            ]"
            :key="item"
            :label="item"
            :value="item"
          />
        </el-select>
        <el-select
          v-model="searchParam.state"
          :placeholder="$t('state')"
          clearable
          style="width: 210px"
          @change="getList"
        >
          <el-option
            v-for="item in [
              'INITIALIZED_STATE',
              'PENDING_STATE',
              'RUNNING_STATE',
              'FINISHED_STATE',
              'TIMEDOUT_STATE',
              'CANCELED_STATE',
              'CLOSED_STATE',
              'ERROR_STATE',
              'UKNOWN_STATE'
            ]"
            :key="item"
            :label="item"
            :value="item"
          />
        </el-select>
        <el-button type="primary" icon="Search" @click="getList" />
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
      <el-table-column
        prop="operationType"
        :label="$t('operation_type')"
        width="160"
      />
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
      <el-table-column fixed="right" :label="$t('operation')" width="144">
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
            <el-tooltip effect="dark" :content="$t('log')" placement="top">
              <el-button
                type="primary"
                icon="Tickets"
                circle
                @click="openLogModal(scope.row.statementId)"
              />
            </el-tooltip>
          </el-space>
        </template>
      </el-table-column>
    </el-table>
    <div class="pagination-container">
      <el-pagination
        v-model:current-page="currentPage"
        v-model:page-size="pageSize"
        :page-sizes="[10, 30, 50]"
        background
        layout="prev, pager, next, sizes, jumper"
        :total="totalPage"
        @size-change="handleSizeChange"
        @current-change="handleCurrentChange"
      />
    </div>
  </el-card>
  <Modal
    v-if="logModalVisible"
    :show="logModalVisible"
    :title="$t('log')"
    :cancel-show="false"
    @cancel="cancelLogModal"
    @confirm="cancelLogModal"
  >
    <div v-loading="logLoading" class="log">{{ log }}</div>
  </Modal>
</template>

<script lang="ts" setup>
  import { ref, reactive } from 'vue'
  import { useRoute } from 'vue-router'
  import {
    getAllOperations,
    getOperationLog,
    cancelOperation
  } from '@/api/session'
  import { IOperationSearch } from '@/api/session/types'
  import { format } from 'date-fns'
  import { useI18n } from 'vue-i18n'
  import { ElMessage } from 'element-plus'
  import { secondTransfer } from '@/utils'
  import { useTable } from '@/views/common/use-table'
  import Modal from '@/components/modal/index.vue'

  const logModalVisible = ref(false)
  const log = ref()
  const logLoading = ref(false)
  const searchParam: IOperationSearch = reactive({
    sessionHandle: null,
    operationType: null,
    state: null
  })
  const route = useRoute()
  const { t } = useI18n()
  searchParam.sessionHandle =
    route.query.sessionId === '' || route.query.sessionId == null
      ? null
      : (route.query.sessionId as string)
  const {
    tableData,
    currentPage,
    pageSize,
    totalPage,
    loading,
    handleSizeChange,
    handleCurrentChange,
    getList: _getList
  } = useTable()

  const openLogModal = (id: string) => {
    logModalVisible.value = true
    logLoading.value = true
    getOperationLog(id)
      .then((res: any) => {
        log.value = Array.isArray(res.logRowSet) ? res.logRowSet.join('\n') : ''
      })
      .catch(() => {
        log.value = ''
      })
      .finally(() => {
        logLoading.value = false
      })
  }

  const cancelLogModal = () => {
    logModalVisible.value = false
  }

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
      getList()
    })
  }

  const getList = () => {
    _getList(getAllOperations, searchParam)
  }

  getList()
</script>
<style lang="scss" scoped>
  header {
    display: flex;
    justify-content: flex-end;
  }

  .log {
    font-family: monospace;
    white-space: pre-line;
  }
</style>
