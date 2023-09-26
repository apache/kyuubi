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
        <el-breadcrumb-item :to="{ path: '/management/session' }">{{
          'Sessions'
        }}</el-breadcrumb-item>
        <el-breadcrumb-item>{{ route.query.sessionId }}</el-breadcrumb-item>
      </el-breadcrumb>
    </header>
  </el-card>
  <el-collapse class="session-properties-container">
    <el-collapse-item name="1">
      <template #title>
        <div class="title">
          <span>{{ $t('session_properties') }}</span>
        </div>
      </template>
      <el-descriptions :column="1" border>
        <div v-for="(p, key) in sessionProperties" :key="key">
          <el-descriptions-item :label="key">
            {{ p }}
          </el-descriptions-item></div
        ></el-descriptions
      >
    </el-collapse-item>
  </el-collapse>
  <el-card class="table-container">
    <template #header>
      <div class="card-header">
        <span>{{ 'Operations' }}</span>
      </div>
    </template>
    <el-table v-loading="loading" :data="tableData" style="width: 100%">
      <el-table-column prop="sessionUser" :label="$t('user')" width="160" />
      <el-table-column
        prop="identifier"
        :label="$t('operation_id')"
        width="300" />
      <el-table-column :label="$t('create_time')" width="160">
        <template #default="scope">
          {{
            scope.row.createTime != null && scope.row.createTime > -1
              ? format(scope.row.createTime, 'yyyy-MM-dd HH:mm:ss')
              : '-'
          }}
        </template>
      </el-table-column>
      <el-table-column :label="$t('complete_time')" width="160">
        <template #default="scope">
          {{
            scope.row.completeTime != null && scope.row.completeTime > -1
              ? format(scope.row.completeTime, 'yyyy-MM-dd HH:mm:ss')
              : '-'
          }}
        </template>
      </el-table-column>
      <el-table-column :label="$t('duration')" width="130">
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
      <el-table-column prop="exception" :label="$t('failure_reason')">
        <template #default="scope">
          {{ scope.row.exception == '' ? '-' : scope.row.exception }}
        </template>
      </el-table-column>
    </el-table>
  </el-card>
</template>

<script lang="ts" setup>
  import { Ref, ref } from 'vue'
  import { getSession, getAllTypeOperation } from '@/api/session'
  import { useRoute } from 'vue-router'
  import { format } from 'date-fns'
  import { secondTransfer } from '@/utils/unit'
  import { useTable } from '@/views/common/use-table'
  const route = useRoute()
  const sessionProperties: Ref<any> = ref({})
  const sessionPropertiesLoading = ref(false)
  const { tableData, loading, getList: _getList } = useTable()

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
      _getList(getAllTypeOperation, sessionId)
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
    margin-bottom: 20px;
    border-radius: 20px;
    .title {
      margin-left: 20px;
    }
  }
</style>
