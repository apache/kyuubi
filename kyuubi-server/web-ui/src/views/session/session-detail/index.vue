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
  <header>
    <el-breadcrumb separator="/">
      <el-breadcrumb-item :to="{ path: '/session/session-statistics' }">{{
        $t('session_statistics')
      }}</el-breadcrumb-item>
      <el-breadcrumb-item>{{ sessionId }}</el-breadcrumb-item>
      <el-breadcrumb-item>{{ $t('detail') }}</el-breadcrumb-item>
    </el-breadcrumb>
  </header>
  <!-- <el-card
    v-loading="sessionPropertiesLoading"
    class="table-container session-properties-container">
    <template #header>
      <div class="card-header">
        <span>{{ $t('session_properties') }}</span>
      </div>
    </template> -->
  <el-collapse class="session-properties-container">
    <el-collapse-item name="1">
      <template #title>
        <div class="collapse-header">
          <span>{{ $t('session_properties') }}</span>
        </div>
      </template>
      <el-descriptions class="margin-top" :column="3" border>
        <div
          v-for="(p, key) in sessionProperties"
          :key="key"
          class="session-property">
          <el-descriptions-item :label="key">
            {{ p }}
          </el-descriptions-item></div
        ></el-descriptions
      >
    </el-collapse-item>
  </el-collapse>
  <!-- </el-card> -->
  <el-card class="table-container">
    <template #header>
      <div class="card-header">
        <span>{{ $t('operation_statistics') }}</span>
      </div>
    </template>
    <el-table
      v-loading="loading"
      :data="tableData"
      style="width: 100%"
      max-height="500px">
      <el-table-column prop="sessionUser" :label="$t('user')" width="160" />
      <el-table-column
        prop="identifier"
        :label="$t('operation_id')"
        width="250" />
      <el-table-column :label="$t('duration')" width="140">
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
      <el-table-column prop="state" :label="$t('state')" width="160">
        <template #default="scope">
          <el-tag
            :type="scope.row.state === 'ERROR_STATE' ? 'danger' : 'success'"
            >{{ scope.row.state }}</el-tag
          >
        </template>
      </el-table-column>
      <el-table-column :label="$t('create_time')" width="180">
        <template #default="scope">
          {{
            scope.row.createTime != null && scope.row.createTime > -1
              ? format(scope.row.createTime, 'yyyy-MM-dd HH:mm:ss')
              : '-'
          }}
        </template>
      </el-table-column>
      <el-table-column :label="$t('start_time')" width="180">
        <template #default="scope">
          {{
            scope.row.startTime != null && scope.row.startTime > -1
              ? format(scope.row.startTime, 'yyyy-MM-dd HH:mm:ss')
              : '-'
          }}
        </template>
      </el-table-column>
      <el-table-column :label="$t('complete_time')" width="180">
        <template #default="scope">
          {{
            scope.row.completeTime != null && scope.row.completeTime > -1
              ? format(scope.row.completeTime, 'yyyy-MM-dd HH:mm:ss')
              : '-'
          }}
        </template>
      </el-table-column>
      <el-table-column
        prop="exception"
        :label="$t('failure_reason')"
        width="160" />
    </el-table>
  </el-card>
</template>
<script lang="ts" setup>
  import { Ref, ref } from 'vue'
  import { getSession } from '@/api/session'
  import { getOperations } from '@/api/operation'
  import { useRoute } from 'vue-router'
  import { format } from 'date-fns'
  import { secondTransfer } from '@/utils/unit'
  import { useTable } from '@/views/common/use-table'

  const route = useRoute()
  const sessionId = route.query.sessionId
  const sessionProperties: Ref<any> = ref({})
  const sessionPropertiesLoading = ref(false)
  const { tableData, loading, getList: _getList } = useTable()

  function getSessionById() {
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
      _getList(getOperations, { sessionHandle: sessionId })
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
    .collapse-header {
      margin-left: 18px;
      display: flex;
      font-size: 16px;
      line-height: 24px;
      font-weight: 400;
      font-family: 'Myriad Pro', 'Helvetica Neue', Arial, Helvetica, sans-serif;
    }
  }
</style>
