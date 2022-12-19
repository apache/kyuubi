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
        <el-input v-model="searchParam" placeholder="Please input" />
        <el-button type="primary" icon="Search" />
      </el-space>
    </header>
  </el-card>
  <el-card class="table-container">
    <el-table v-loading="loading" :data="tableData" style="width: 100%">
      <el-table-column prop="host" :label="$t('server_ip')" min-width="20%" />
      <el-table-column
        prop="engineType"
        :label="$t('engine_type')"
        min-width="20%"
      />
      <el-table-column
        prop="sharelevel"
        :label="$t('share_level')"
        min-width="20%"
      />
      <el-table-column
        prop="memoryTotal"
        :label="$t('memory')"
        min-width="20%"
      />
      <el-table-column prop="user" :label="$t('user')" min-width="20%" />
      <el-table-column :label="$t('start_time')" min-width="20%">
        <template #default="scope">
          {{
            scope.row.createTime != null && Number(scope.row.createTime) > 0
              ? format(Number(scope.row.createTime), 'yyyy-MM-dd HH:mm:ss')
              : '-'
          }}
        </template>
      </el-table-column>
      <el-table-column prop="cpuTotal" :label="$t('cpu')" min-width="20%" />
      <el-table-column
        prop="memoryTotal"
        :label="$t('memory')"
        min-width="20%"
      />
      <el-table-column prop="status" :label="$t('status')" min-width="20%" />
      <el-table-column fixed="right" :label="$t('operation')" width="160">
        <template #default>
          <el-space wrap>
            <el-tooltip
              effect="dark"
              :content="$t('view_config')"
              placement="top"
            >
              <el-button type="primary" icon="Setting" circle />
            </el-tooltip>
          </el-space>
        </template>
      </el-table-column>
    </el-table>
  </el-card>
</template>

<script lang="ts" setup>
  import { format } from 'date-fns'
  import { getAllEngines } from '@/api/server'
  import { useTable } from '@/views/common/use-table'

  const { tableData, loading, searchParam, getList } = useTable()

  getList(getAllEngines)
</script>

<style scoped lang="scss">
  header {
    display: flex;
    justify-content: flex-end;
  }
</style>
