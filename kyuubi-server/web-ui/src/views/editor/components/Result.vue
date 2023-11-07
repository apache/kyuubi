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
  <div class="result">
    <el-table
      v-if="data?.length"
      v-fit-columns
      stripe
      :data="data"
      class="editor-el-table">
      <el-table-column type="index" width="50" />
      <el-table-column
        v-for="(_value, key) in data[0]"
        :key="key"
        :prop="key"
        :label="key"
        show-overflow-tooltip
        sortable />
    </el-table>
    <template v-else>
      <template v-if="errorMessages.length">
        <el-alert
          v-for="(m, idx) in errorMessages"
          :key="idx"
          :title="m.title"
          type="error"
          show-icon>
          <template #default>
            <pre> {{ m.description }} </pre>
          </template>
        </el-alert>
      </template>
      <div v-else class="no-data">
        <img src="@/assets/images/document.svg" />
        <div>{{ data === null ? $t('run_sql_tips') : $t('no_data') }}</div>
      </div>
    </template>
  </div>
</template>

<script lang="ts" setup>
  import { PropType } from 'vue'
  import type { IErrorMessage } from './types'
  defineProps({
    data: {
      type: [Array, null] as PropType<Array<any> | null>,
      default: null,
      required: true
    },
    errorMessages: {
      type: Array as PropType<Array<IErrorMessage>>,
      default: [],
      required: true
    }
  })

  function adjustColumnWidth(table: any, padding = 12) {
    const colgroup = table.querySelector('colgroup')
    const colDefs = [...colgroup.querySelectorAll('col')]
    colDefs.forEach((col) => {
      const clsName = col.getAttribute('name')
      // ...table.querySelectorAll(`td.${clsName}`),
      const cells = [...table.querySelectorAll(`th.${clsName}`)]
      if (cells[0]?.classList?.contains?.('leave-alone')) {
        return
      }
      const widthList = cells.map((el) => {
        return el.querySelector('.cell')?.scrollWidth || 0
      })
      const max = Math.max(...widthList)
      table.querySelectorAll(`col[name=${clsName}]`).forEach((el: any) => {
        el.setAttribute('width', max + padding)
      })
      table.querySelectorAll(`td.${clsName} .cell`).forEach((el: any) => {
        el.style.width = max + padding + 'px'
      })
    })
  }

  const vFitColumns = {
    mounted(el: any, binding: any) {
      setTimeout(() => {
        adjustColumnWidth(el, binding.value)
      }, 200)
    },
    updated(el: any, binding: any) {
      el.classList.add('r-table')
      setTimeout(() => {
        adjustColumnWidth(el, binding.value)
      }, 200)
    }
  }
</script>

<style lang="scss" scoped>
  :deep(.editor-el-table.r-table) {
    th .cell {
      display: inline-block;
      white-space: nowrap;
      width: auto;
      overflow: auto;
    }
    .el-table__body-wrapper {
      overflow-x: auto;
    }
  }
  .result {
    min-height: 200px;
    position: relative;
    .el-alert {
      width: auto;
      margin: 10px;
      border: 1px solid #db2828;

      :deep(.el-alert__description) {
        max-height: 300px;
        overflow: auto;

        pre {
          margin-top: 0;
          white-space: pre-wrap;
        }
      }
    }
    .no-data {
      position: absolute;
      left: 50%;
      top: 50%;
      transform: translate(-50%, -50%);
      font-size: 14px;
      color: #999;
      font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;
      text-align: center;
    }
  }
</style>
