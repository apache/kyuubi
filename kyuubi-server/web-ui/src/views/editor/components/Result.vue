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
    <el-table v-if="data?.length" stripe :data="data" class="editor-el-table">
      <el-table-column type="index" class-name="index" :width="indexWidth" />
      <el-table-column
        v-for="(width, key) in columns"
        :key="key"
        :prop="key"
        :label="key"
        :width="width"
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
  import { PropType, computed } from 'vue'
  import type { IErrorMessage } from './types'

  const font = '14px Myriad Pro,Helvetica Neue,Arial,Helvetica,sans-serif'
  const props = defineProps({
    data: {
      type: [Array, null] as PropType<Array<Object> | null>,
      default: null,
      required: true
    },
    errorMessages: {
      type: Array as PropType<Array<IErrorMessage>>,
      default: [],
      required: true
    }
  })

  const measureTextWidth = (text: string, font: string, deviation = 48) => {
    const canvas = document.createElement('canvas')
    const context: CanvasRenderingContext2D = canvas.getContext(
      '2d'
    ) as CanvasRenderingContext2D
    context.font = font
    const width = context.measureText(text).width
    return width + deviation
  }

  const indexWidth = computed(() =>
    props.data?.length ? measureTextWidth(String(props.data?.length), font) : 0
  )

  const columns = computed(() => {
    if (props.data?.length) {
      const maxColumnLength = 600
      const obj: { [key: string]: number } = {}
      props.data?.forEach((item: { [key: string]: any }) => {
        for (const key in item) {
          if (!obj[key]) {
            obj[key] = measureTextWidth(key, font, 80)
          }
          obj[key] = Math.min(
            maxColumnLength,
            Math.max(obj[key], measureTextWidth(String(item[key]), font))
          )
        }
      })
      return obj
    } else {
      return {}
    }
  })
</script>

<style lang="scss" scoped>
  @import '../styles/shared-styles.scss';

  :deep(.editor-el-table) {
    .el-table__header,
    .el-table__body,
    .el-table__body-wrapper .el-scrollbar__view,
    tbody td .cell {
      min-width: 100%;
    }

    tbody td.index .cell {
      text-align: center;
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
      @include sharedNoData;
    }
  }
</style>
