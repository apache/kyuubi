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
          v-model="searchParam.host"
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
      <el-table-column prop="host" :label="$t('server_ip')" min-width="20%" />
      <el-table-column prop="cpuTotal" :label="$t('cpu')" min-width="20%" />
      <el-table-column prop="memoryTotal" :label="$t('memory')" min-width="20%">
        <template #default="scope">
          <span>{{
            scope.row.memoryTotal == null || scope.row.memoryTotal === ''
              ? '-'
              : byteTransfer(scope.row.memoryTotal)
          }}</span>
        </template>
      </el-table-column>
      <el-table-column :label="$t('start_time')" min-width="20%">
        <template #default="scope">
          {{
            scope.row.createTime != null && scope.row.createTime > 0
              ? format(scope.row.createTime, 'yyyy-MM-dd HH:mm:ss')
              : '-'
          }}
        </template>
      </el-table-column>
      <el-table-column prop="status" :label="$t('status')" min-width="20%" />
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
  import { reactive } from 'vue'
  import { format } from 'date-fns'
  import { getAllServers } from '@/api/server'
  import { useTable } from '@/views/common/use-table'
  import { byteTransfer } from '@/utils'

  const searchParam = reactive({
    host: null
  })

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

  const getList = () => {
    _getList(getAllServers, searchParam)
  }

  getList()
</script>

<style scoped lang="scss">
  header {
    display: flex;
    justify-content: flex-end;
  }
</style>
