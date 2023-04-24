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
  <el-card class="table-container">
    <el-table v-loading="loading" :data="tableData" style="width: 100%">
      <el-table-column prop="host" :label="$t('server_ip')" min-width="20%" />
      <el-table-column
        prop="namespace"
        :label="$t('namespace')"
        min-width="20%" />
      <el-table-column
        prop="instance"
        :label="$t('kyuubi_instance')"
        min-width="20%" />
      <el-table-column prop="attributes.version" :label="$t('version')" />
      <el-table-column prop="status" :label="$t('state')" min-width="20%" />
    </el-table>
  </el-card>
</template>

<script lang="ts" setup>
  import { getAllServer } from '@/api/server'
  import { useTable } from '@/views/common/use-table'

  const { tableData, loading, getList: _getList } = useTable()
  const getList = () => {
    _getList(getAllServer)
  }
  getList()
</script>

<style scoped lang="scss">
  header {
    display: flex;
    justify-content: flex-end;
  }
</style>
