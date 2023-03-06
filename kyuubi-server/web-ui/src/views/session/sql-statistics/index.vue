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
      <el-breadcrumb separator="/">
        <el-breadcrumb-item :to="{ path: '/session/session-statistics' }">{{
          $t('session_statistics')
        }}</el-breadcrumb-item>
        <el-breadcrumb-item>{{ $t('sql_statistics') }}</el-breadcrumb-item>
      </el-breadcrumb>
    </header>
  </el-card>
  <el-card
    v-loading="sessionPropertiesLoading"
    class="table-container session-properties-container"
  >
    <template #header>
      <div class="card-header">
        <span>{{ $t('session_properties') }}</span>
      </div>
    </template>
    <div class="main">
      <div
        v-for="(p, key) in sessionProperties"
        :key="key"
        class="session-property"
        ><el-tag>{{ `${key} : ${p}` }}</el-tag></div
      >
    </div>
  </el-card>
  <el-card class="table-container">
    <template #header>
      <div class="card-header">
        <span>{{ $t('sql_details') }}</span>
      </div>
    </template>
    <el-table v-loading="loading" :data="tableData" style="width: 100%">
      <el-table-column prop="sessionUser" :label="$t('user')" width="160" />
      <el-table-column
        prop="statementId"
        :label="$t('statement_id')"
        width="160"
      />
      <el-table-column :label="$t('create_time')" width="200">
        <template #default="scope">
          {{
            scope.row.createTime != null && scope.row.createTime > -1
              ? format(scope.row.createTime, 'yyyy-MM-dd HH:mm:ss')
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
          scope.row.createTime != null &&
          scope.row.completeTime != null &&
          scope.row.createTime > -1 &&
          scope.row.completeTime > -1
            ? secondTransfer(
                (scope.row.completeTime - scope.row.createTime) / 1000
              )
            : '-'
        }}</template>
      </el-table-column>
      <el-table-column prop="statement" :label="$t('statement')" width="160" />
      <el-table-column prop="engineId" :label="$t('engine_id')" width="160" />
      <el-table-column
        prop="engineType"
        :label="$t('engine_type')"
        width="160"
      />
      <el-table-column
        prop="engineShareLevel"
        :label="$t('engine_share_level')"
        width="160"
      />
      <el-table-column
        prop="exception"
        :label="$t('failure_reason')"
        width="160"
      />
      <el-table-column fixed="right" :label="$t('operation')" width="120">
        <template #default="scope">
          <el-tooltip effect="dark" :content="$t('operation')" placement="top">
            <el-button
              type="primary"
              icon="Operation"
              circle
              @click="openOperationPage(scope.row.sessionId)"
            />
          </el-tooltip>
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
</template>

<script lang="ts" setup>
  import { Ref, ref } from 'vue'
  import { getSession, getSqlDetails } from '@/api/session'
  import { useRoute, useRouter, Router } from 'vue-router'
  import { format } from 'date-fns'
  import { secondTransfer } from '@/utils'
  import { useTable } from '@/views/common/use-table'

  const route = useRoute()
  const router: Router = useRouter()
  const sessionProperties: Ref<any> = ref({})
  const sessionPropertiesLoading = ref(false)
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

  const openOperationPage = (sessionId: string) => {
    router.push({
      path: '/session/operation',
      query: {
        sessionId
      }
    })
  }

  const getSessionById = () => {
    const sessionId = route.query.sessionId
    if (sessionId) {
      sessionPropertiesLoading.value = true
      getSession(sessionId as string)
        .then((res: any) => {
          sessionProperties.value = res?.conf || {}
        })
        .finally(() => {
          sessionPropertiesLoading.value = false
        })
    }
  }

  const getList = () => {
    const sessionId = route.query.sessionId
    if (sessionId) {
      _getList(getSqlDetails, sessionId)
    }
  }

  getSessionById()
  getList()
</script>
<style lang="scss" scoped>
  header {
    display: flex;
    justify-content: space-between;
    .el-breadcrumb {
      line-height: 32px;
    }
  }

  .session-properties-container {
    max-height: 400px;
    overflow: auto;

    .main {
      display: flex;
      flex-wrap: wrap;
      .session-property {
        margin-bottom: 6px;
        margin-right: 6px;
      }
    }
  }
</style>
