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
  <el-card :body-style="{ padding: '10px 14px' }" class="filter_card">
    <header>
      <el-space class="search-box">
        <el-select
          v-model="searchParam.type"
          :placeholder="$t('engine_type')"
          clearable
          style="width: 210px"
          @change="getList">
          <el-option
            v-for="item in getEngineType()"
            :key="item"
            :label="item"
            :value="item" />
        </el-select>
        <el-select
          v-model="searchParam.sharelevel"
          :placeholder="$t('share_level')"
          clearable
          style="width: 210px"
          @change="getList">
          <el-option
            v-for="item in getShareLevel()"
            :key="item"
            :label="item"
            :value="item" />
        </el-select>
        <el-input
          v-model="searchParam['hive.server2.proxy.user']"
          :placeholder="$t('user')"
          style="width: 210px"
          @keyup.enter="getList" />
        <el-button type="primary" icon="Search" @click="getList" />
      </el-space>
    </header>
  </el-card>
  <el-card class="table-container">
    <el-table v-loading="loading" :data="tableData" style="width: 100%">
      <el-table-column
        prop="instance"
        :label="$t('engine_address')"
        min-width="20%" />
      <el-table-column :label="$t('engine_id')" min-width="20%">
        <template #default="scope">
          <span>{{
            scope.row.attributes && scope.row.attributes['kyuubi.engine.id']
              ? scope.row.attributes['kyuubi.engine.id']
              : '-'
          }}</span>
        </template>
      </el-table-column>
      <el-table-column
        prop="engineType"
        :label="$t('engine_type')"
        min-width="20%" />
      <el-table-column
        prop="sharelevel"
        :label="$t('share_level')"
        min-width="20%" />

      <el-table-column prop="user" :label="$t('user')" min-width="15%" />
      <el-table-column prop="version" :label="$t('version')" min-width="15%" />
      <el-table-column fixed="right" :label="$t('operation.text')" width="120">
        <template #default="scope">
          <el-space wrap>
            <el-tooltip
              effect="dark"
              :content="
                $t('engine_ui') +
                ': ' +
                scope.row.attributes['kyuubi.engine.url']
              "
              placement="top">
              <el-button
                type="primary"
                icon="Link"
                circle
                @click="
                  openEngineUI(scope.row.attributes['kyuubi.engine.url'])
                " />
            </el-tooltip>
            <el-popconfirm
              :title="$t('operation.delete_confirm')"
              @confirm="handleDeleteEngine(scope.row)">
              <template #reference>
                <span>
                  <el-tooltip
                    effect="dark"
                    :content="$t('operation.delete')"
                    placement="top">
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
  </el-card>
</template>

<script lang="ts" setup>
  import { reactive } from 'vue'
  import { getAllEngines, deleteEngine } from '@/api/engine'
  import { IEngineSearch } from '@/api/engine/types'
  import { useTable } from '@/views/common/use-table'
  import { ElMessage } from 'element-plus'
  import { useI18n } from 'vue-i18n'
  import { getEngineType, getShareLevel } from '@/utils/engine'

  const { t } = useI18n()
  const { tableData, loading, getList: _getList } = useTable()
  // default search params
  const searchParam: IEngineSearch = reactive({
    type: 'SPARK_SQL',
    sharelevel: 'USER',
    'hive.server2.proxy.user': 'anonymous'
  })
  const getList = () => {
    _getList(getAllEngines, searchParam)
  }
  const init = () => {
    getList()
  }

  function handleDeleteEngine(row: any) {
    deleteEngine({
      type: row?.engineType,
      sharelevel: row?.sharelevel,
      'hive.server2.proxy.user': row?.user,
      subdomain: row?.subdomain
    })
      .then(() => {
        ElMessage({
          message: t('delete_succeeded', { name: 'engine' }),
          type: 'success'
        })
      })
      .catch(() => {
        ElMessage({
          message: t('delete_failed', { name: 'engine' }),
          type: 'error'
        })
      })
      .finally(() => {
        getList()
      })
  }

  function getProxyEngineUI(url: string): string {
    url = (url || '').replaceAll(/http:|https:/gi, '')
    return `${import.meta.env.VITE_APP_DEV_WEB_URL}engine-ui/${url}/`
  }

  function openEngineUI(url: string) {
    window.open(getProxyEngineUI(url))
  }

  init()
  // export for test
  defineExpose({
    getProxyEngineUI
  })
</script>

<style scoped lang="scss">
  header {
    display: flex;
    justify-content: flex-end;
  }
  .filter_card {
    margin-bottom: 10px;
  }
</style>
