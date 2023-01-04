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
          v-model="searchParam.user"
          :placeholder="$t('user')"
          style="width: 210px"
          @keyup.enter="getList"
        />
        <el-input
          v-model="searchParam.serverIP"
          :placeholder="$t('server_ip')"
          style="width: 210px"
          @keyup.enter="getList"
        />
        <el-button type="primary" icon="Search" @click="getList" />
      </el-space>
    </header>
  </el-card>
  <el-card class="table-container">
    <el-table v-loading="loading" :data="tableData" style="width: 100%">
      <el-table-column prop="user" :label="$t('user')" width="160" />
      <el-table-column prop="clientIP" :label="$t('client_ip')" width="160" />
      <el-table-column prop="serverIP" :label="$t('server_ip')" width="180" />
      <el-table-column :label="$t('session_id')" width="160">
        <template #default="scope">
          <el-link @click="handleSessionJump(scope.row.sessionId)">{{
            scope.row.sessionId
          }}</el-link>
        </template>
      </el-table-column>
      <el-table-column
        prop="sessionName"
        :label="$t('session_name')"
        width="160"
      />
      <el-table-column
        prop="runningOperations"
        :label="$t('running_operations')"
        width="120"
      />
      <el-table-column
        prop="errorOperations"
        :label="$t('error_operations')"
        width="120"
      />
      <el-table-column
        prop="finishedOperations"
        :label="$t('finish_operations')"
        width="120"
      />
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
            scope.row.endTime != null && scope.row.endTime > -1
              ? format(scope.row.endTime, 'yyyy-MM-dd HH:mm:ss')
              : '-'
          }}
        </template>
      </el-table-column>
      <el-table-column :label="$t('duration')" width="200">
        <template #default="scope">{{
          scope.row.startTime != null &&
          scope.row.endTime != null &&
          scope.row.startTime > -1 &&
          scope.row.endTime > -1
            ? secondTransfer((scope.row.endTime - scope.row.startTime) / 1000)
            : '-'
        }}</template>
      </el-table-column>
      <el-table-column fixed="right" :label="$t('operation')" width="120">
        <template #default="scope">
          <el-popconfirm
            :title="$t('delete_confirm')"
            @confirm="handleDeleteSession(scope.row.sessionId)"
          >
            <template #reference>
              <span>
                <el-tooltip
                  effect="dark"
                  :content="$t('delete')"
                  placement="top"
                >
                  <template #default>
                    <el-button type="danger" icon="Delete" circle />
                  </template> </el-tooltip
              ></span>
            </template>
          </el-popconfirm>
        </template>
      </el-table-column>
    </el-table>
  </el-card>
</template>

<script lang="ts" setup>
  import { reactive } from 'vue'
  import { format } from 'date-fns'
  import { secondTransfer } from '@/utils'
  import { getAllSessions, deleteSession } from '@/api/session'
  import { Router, useRouter } from 'vue-router'
  import { ElMessage } from 'element-plus'
  import { useI18n } from 'vue-i18n'
  import { useTable } from '@/views/common/use-table'

  const router: Router = useRouter()
  const { t } = useI18n()
  const searchParam = reactive({
    user: null,
    serverIP: null
  })
  const { tableData, loading, getList: _getList } = useTable()

  const handleSessionJump = (sessionId: string) => {
    router.push({
      path: '/session/sql-statistics',
      query: {
        sessionId
      }
    })
  }

  const handleDeleteSession = (sessionId: string) => {
    deleteSession(sessionId).then(() => {
      // need add delete success or failed logic after api support
      getList()
      ElMessage({
        message: t('delete_success'),
        type: 'success'
      })
    })
  }

  const getList = () => {
    _getList(getAllSessions, searchParam)
  }

  getList()
</script>

<style scoped lang="scss">
  header {
    display: flex;
    justify-content: flex-end;
  }
</style>
