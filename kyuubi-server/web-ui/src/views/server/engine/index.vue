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
        <el-select
          v-model="searchParam.engineType"
          :placeholder="$t('engine_type')"
          clearable
          style="width: 210px"
          @change="getList"
        >
          <el-option
            v-for="item in [
              'SPARK_SQL',
              'FLINK_SQL',
              'TRINO',
              'HIVE_SQL',
              'JDBC'
            ]"
            :key="item"
            :label="item"
            :value="item"
          />
        </el-select>
        <el-select
          v-model="searchParam.sharelevel"
          :placeholder="$t('share_level')"
          clearable
          style="width: 210px"
          @change="getList"
        >
          <el-option
            v-for="item in ['CONNECTION', 'USER', 'GROUP', 'SERVER']"
            :key="item"
            :label="item"
            :value="item"
          />
        </el-select>
        <el-input
          v-model="searchParam['hive.server2.proxy.user']"
          :placeholder="$t('user')"
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
        :label="$t('engine_id')"
        min-width="20%"
      >
        <template #default="scope">
          <span>{{
            scope.row.attributes && scope.row.attributes['kyuubi.engine.id']
              ? scope.row.attributes['kyuubi.engine.id']
              : '-'
          }}</span>
        </template>
      </el-table-column>
      <el-table-column prop="user" :label="$t('user')" min-width="20%" />
      <el-table-column :label="$t('start_time')" min-width="30%">
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
      >
        <template #default="scope">
          <span>{{
            scope.row.memoryTotal == null || scope.row.memoryTotal === ''
              ? '-'
              : byteTransfer(scope.row.memoryTotal)
          }}</span>
        </template>
        </el-table-column>
      <el-table-column prop="status" :label="$t('status')" min-width="20%" />
      <el-table-column fixed="right" :label="$t('operation')" width="120">
        <template #default="scope">
          <el-space wrap>
            <el-tooltip
              effect="dark"
              :content="$t('engine_ui')"
              placement="top"
            >
              <el-button
                type="primary"
                icon="Link"
                circle
                @click="openEngineUi(scope.row.url)"
              />
            </el-tooltip>
            <el-popconfirm
              :title="$t('delete_confirm')"
              @confirm="handleDeleteEngine(scope.row)"
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
                    </template>
                  </el-tooltip>
                </span>
              </template>
            </el-popconfirm>
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
</template>

<script lang="ts" setup>
  import { reactive } from 'vue'
  import { format } from 'date-fns'
  import { getAllEngines, deleteEngine } from '@/api/server'
  import { IEngineSearch } from '@/api/server/types'
  import { useTable } from '@/views/common/use-table'
  import { ElMessage } from 'element-plus'
  import { useI18n } from 'vue-i18n'
  import { byteTransfer } from '@/utils'

  const { t } = useI18n()
  const searchParam: IEngineSearch = reactive({
    engineType: null,
    sharelevel: null,
    'hive.server2.proxy.user': null
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

  const openEngineUi = (url: string) => {
    url = (url || '').replaceAll(/http:|https:/gi, '')
    window.open(`${import.meta.env.VITE_APP_DEV_WEB_URL}engine-ui/${url}/`)
  }

  const handleDeleteEngine = (row: any) => {
    deleteEngine({
      engineType: row?.engineType,
      sharelevel: row?.sharelevel,
      'hive.server2.proxy.user': row?.user,
      subdomain: row?.subdomain
    }).then(() => {
      // need add delete success or failed logic after api support
      getList()
      ElMessage({
        message: t('delete_success'),
        type: 'success'
      })
    })
  }

  const getList = () => {
    _getList(getAllEngines, searchParam)
  }

  getList()
</script>

<style scoped lang="scss">
  header {
    display: flex;
    justify-content: flex-end;
  }
</style>
