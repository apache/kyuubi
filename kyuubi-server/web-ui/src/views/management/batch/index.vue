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
  <el-card :body-style="{ padding: '10px 14px' }" class="filter-card">
    <header>
      <el-space class="search-box" wrap>
        <el-input
          v-model="searchParam.batchUser"
          :placeholder="$t('user')"
          clearable
          style="width: 180px"
          @keyup.enter="searchBatches" />
        <el-input
          v-model="searchParam.batchName"
          :placeholder="$t('batch_name')"
          clearable
          style="width: 180px"
          @keyup.enter="searchBatches" />
        <el-select
          v-model="searchParam.batchType"
          :placeholder="$t('batch_type')"
          clearable
          style="width: 150px"
          @change="searchBatches">
          <el-option label="SPARK" value="SPARK" />
          <el-option label="PYSPARK" value="PYSPARK" />
        </el-select>
        <el-select
          v-model="searchParam.batchState"
          :placeholder="$t('state')"
          clearable
          style="width: 150px"
          @change="searchBatches">
          <el-option
            v-for="state in batchStates"
            :key="state"
            :label="state"
            :value="state" />
        </el-select>
        <el-button type="primary" icon="Search" @click="searchBatches" />
        <el-button icon="Refresh" @click="getList" />
      </el-space>
    </header>
  </el-card>

  <el-card class="table-container">
    <el-table
      v-loading="loading"
      class="batch-table"
      :data="tableData"
      style="width: 100%">
      <el-table-column prop="user" :label="$t('user')" min-width="120" />
      <el-table-column :label="$t('batch_id')" min-width="300">
        <template #default="scope">
          <span class="truncate-text">{{ scope.row.id }}</span>
        </template>
      </el-table-column>
      <el-table-column :label="$t('batch_name')" min-width="180">
        <template #default="scope">{{ display(scope.row.name) }}</template>
      </el-table-column>
      <el-table-column
        prop="batchType"
        :label="$t('batch_type')"
        min-width="130" />
      <el-table-column prop="state" :label="$t('state')" min-width="120" />
      <el-table-column :label="$t('app_state')" min-width="170">
        <template #default="scope">{{ display(scope.row.appState) }}</template>
      </el-table-column>
      <el-table-column :label="$t('app_id')" min-width="210">
        <template #default="scope">{{ display(scope.row.appId) }}</template>
      </el-table-column>
      <el-table-column
        prop="kyuubiInstance"
        :label="$t('kyuubi_instance')"
        min-width="220"
        show-overflow-tooltip />
      <el-table-column :label="$t('create_time')" min-width="170">
        <template #default="scope">{{
          formatTime(scope.row.createTime)
        }}</template>
      </el-table-column>
      <el-table-column :label="$t('end_time')" min-width="170">
        <template #default="scope">{{
          formatTime(scope.row.endTime)
        }}</template>
      </el-table-column>
      <el-table-column :label="$t('duration')" min-width="130">
        <template #default="scope">{{ formatDuration(scope.row) }}</template>
      </el-table-column>
      <el-table-column :label="$t('app_diagnostic')" min-width="240">
        <template #default="scope">
          <el-tooltip
            v-if="scope.row.appDiagnostic"
            effect="dark"
            :content="scope.row.appDiagnostic"
            placement="top">
            <span class="truncate-text">{{ scope.row.appDiagnostic }}</span>
          </el-tooltip>
          <span v-else>-</span>
        </template>
      </el-table-column>
      <el-table-column fixed="right" :label="$t('operation.text')" width="160">
        <template #default="scope">
          <el-space wrap>
            <el-tooltip
              v-if="canShowAppUI(scope.row)"
              effect="dark"
              :content="$t('app_url') + ': ' + getAppUI(scope.row.appUrl)"
              placement="top">
              <el-button
                type="primary"
                icon="Link"
                circle
                @click="openAppUI(scope.row.appUrl)" />
            </el-tooltip>
            <el-tooltip effect="dark" :content="$t('log')" placement="top">
              <el-button icon="Document" circle @click="openLog(scope.row)" />
            </el-tooltip>
            <el-popconfirm
              v-if="!isTerminalState(scope.row.state)"
              :title="$t('operation.cancel_confirm')"
              @confirm="handleDeleteBatch(scope.row)">
              <template #reference>
                <span>
                  <el-tooltip
                    effect="dark"
                    :content="$t('operation.cancel')"
                    placement="top">
                    <template #default>
                      <el-button type="danger" icon="Remove" circle />
                    </template>
                  </el-tooltip>
                </span>
              </template>
            </el-popconfirm>
          </el-space>
        </template>
      </el-table-column>
    </el-table>
    <footer class="pager">
      <span>{{ pageRangeText }}</span>
      <el-space>
        <el-button
          icon="ArrowLeft"
          :disabled="!hasPreviousPage || loading"
          @click="previousPage">
          {{ $t('previous') }}
        </el-button>
        <el-button
          icon="ArrowRight"
          :disabled="!hasNextPage || loading"
          @click="nextPage">
          {{ $t('next') }}
        </el-button>
      </el-space>
    </footer>
  </el-card>

  <el-drawer
    v-model="logDrawerVisible"
    :title="selectedBatch ? `${$t('log')} - ${selectedBatch.id}` : $t('log')"
    size="60%"
    @close="onLogDrawerClose">
    <div class="log-body">
      <div class="log-toolbar">
        <el-switch
          v-model="logFollowing"
          :disabled="logFollowDisabled"
          :active-text="$t('follow')"
          @change="onFollowChange" />
        <el-button
          icon="Refresh"
          :disabled="logFollowing || logLoading"
          @click="refreshLog">
          {{ $t('refresh') }}
        </el-button>
        <span v-if="logTruncated" class="log-truncated">
          {{ $t('log_truncated', { count: LOG_MAX_ROWS }) }}
        </span>
      </div>
      <el-scrollbar
        ref="logScrollbarRef"
        v-loading="logLoading"
        class="log-container">
        <pre v-if="batchLogs.length > 0">{{ batchLogs.join('\n') }}</pre>
        <el-empty v-else :description="$t('no_log')" />
      </el-scrollbar>
    </div>
  </el-drawer>
</template>

<script lang="ts" setup>
  import { computed, nextTick, onBeforeUnmount, reactive, ref } from 'vue'
  import { format } from 'date-fns'
  import { ElMessage } from 'element-plus'
  import { useI18n } from 'vue-i18n'
  import {
    deleteBatch,
    getAllBatches,
    getBatch,
    getBatchLocalLog
  } from '@/api/batch'
  import { IBatch, IBatchSearch } from '@/api/batch/types'
  import { getWebUIConfig } from '@/api/server'
  import { IWebUIConfig } from '@/api/server/types'
  import { getEngineUIUrl } from '@/utils/engine-ui'
  import { millTransfer } from '@/utils/unit'

  const { t } = useI18n()
  const loading = ref(false)
  const tableData = ref<IBatch[]>([])
  const pageSize = ref(100)
  const logDrawerVisible = ref(false)
  const logLoading = ref(false)
  const batchLogs = ref<string[]>([])
  const selectedBatch = ref<IBatch>()
  const logFollowing = ref(false)
  const logTruncated = ref(false)
  const logScrollbarRef = ref()
  const logNextFrom = ref(0)
  let logTimer: ReturnType<typeof setTimeout> | undefined
  // Bumped whenever the log view is reset (open / refresh / close) so in-flight
  // fetches from a previous view discard their results instead of appending.
  let logEpoch = 0

  const batchStates = ['PENDING', 'RUNNING', 'FINISHED', 'ERROR', 'CANCELED']
  const terminalBatchStates = new Set(['FINISHED', 'ERROR', 'CANCELED'])

  // Batch log viewer: fetch the operation log by absolute line offset (server
  // OperationLog.read(from, size) treats `from` as a line index), tracking the
  // cursor on the client so a live tail never fights the shared server cursor.
  const LOG_FETCH_SIZE = 1000
  const LOG_POLL_INTERVAL_MS = 2000
  const LOG_MAX_ROWS = 5000

  const searchParam: IBatchSearch = reactive({
    from: 0,
    size: 100,
    desc: true
  })

  const hasPreviousPage = computed(() => (searchParam.from || 0) > 0)
  const hasNextPage = computed(() => tableData.value.length === pageSize.value)
  const pageRangeText = computed(() => {
    const from = searchParam.from || 0
    if (tableData.value.length === 0) {
      return '0 - 0'
    }
    return `${from + 1} - ${from + tableData.value.length}`
  })

  const engineUIProxyConfig: IWebUIConfig = reactive({
    engineUIProxyEnabled: false
  })

  function display(value: string | null | undefined): string {
    return value ? value : '-'
  }

  function formatTime(value: number): string {
    return value != null && value > 0
      ? format(value, 'yyyy-MM-dd HH:mm:ss')
      : '-'
  }

  function formatDuration(batch: IBatch): string {
    return batch.createTime > 0 && batch.endTime > 0
      ? millTransfer(batch.endTime - batch.createTime)
      : '-'
  }

  function isTerminalState(state: string): boolean {
    return terminalBatchStates.has(state)
  }

  // The driver UI is only reachable while the engine is alive; once the batch is
  // terminal the app/driver is gone, so hide the link rather than open a 502.
  function canShowAppUI(batch: IBatch): boolean {
    return Boolean(batch.appUrl) && !isTerminalState(batch.state)
  }

  function getErrorMessage(error: unknown, fallback: string): string {
    if (error instanceof Error && error.message) {
      return error.message
    }
    return fallback
  }

  function getAppUI(url: string): string {
    return getEngineUIUrl(url, engineUIProxyConfig.engineUIProxyEnabled)
  }

  function openAppUI(url: string) {
    window.open(getAppUI(url))
  }

  function getList() {
    loading.value = true
    searchParam.size = pageSize.value
    getAllBatches(searchParam)
      .then((res) => {
        tableData.value = res.batches || []
      })
      .catch((error) => {
        tableData.value = []
        ElMessage({
          message: getErrorMessage(error, t('message.get_batches_failed')),
          type: 'error'
        })
      })
      .finally(() => {
        loading.value = false
      })
  }

  function searchBatches() {
    searchParam.from = 0
    getList()
  }

  function previousPage() {
    searchParam.from = Math.max((searchParam.from || 0) - pageSize.value, 0)
    getList()
  }

  function nextPage() {
    searchParam.from = (searchParam.from || 0) + pageSize.value
    getList()
  }

  // Following is only meaningful while the batch can still produce new logs.
  const logFollowDisabled = computed(
    () => !selectedBatch.value || isTerminalState(selectedBatch.value.state)
  )

  function scrollLogToBottom(): void {
    nextTick(() => {
      // el-scrollbar (Element Plus 2.2.x) exposes setScrollTop; assigning a value
      // past the maximum scrolls to the bottom (the browser clamps scrollTop).
      logScrollbarRef.value?.setScrollTop(Number.MAX_SAFE_INTEGER)
    })
  }

  function appendLogRows(rows: string[]): void {
    if (rows.length === 0) {
      return
    }
    const merged = batchLogs.value.concat(rows)
    logNextFrom.value += rows.length
    if (merged.length > LOG_MAX_ROWS) {
      batchLogs.value = merged.slice(merged.length - LOG_MAX_ROWS)
      logTruncated.value = true
    } else {
      batchLogs.value = merged
    }
  }

  function fetchLogChunk(): Promise<number> {
    const batch = selectedBatch.value
    if (!batch) {
      return Promise.resolve(0)
    }
    const epoch = logEpoch
    return getBatchLocalLog(batch.id, logNextFrom.value, LOG_FETCH_SIZE).then(
      (res) => {
        // Discard the result if the view was reset while this request was in flight.
        if (epoch !== logEpoch) {
          return 0
        }
        const rows = res.logRowSet || []
        appendLogRows(rows)
        if (logFollowing.value) {
          scrollLogToBottom()
        }
        return rows.length
      }
    )
  }

  // Read forward until caught up with the current end of the log.
  function drainLogs(): Promise<void> {
    return fetchLogChunk().then((count) => {
      if (count >= LOG_FETCH_SIZE && logDrawerVisible.value) {
        return drainLogs()
      }
      return Promise.resolve()
    })
  }

  function stopFollowing(): void {
    logFollowing.value = false
    if (logTimer) {
      clearTimeout(logTimer)
      logTimer = undefined
    }
  }

  function refreshBatchState(): Promise<boolean> {
    const batch = selectedBatch.value
    if (!batch) {
      return Promise.resolve(true)
    }
    return getBatch(batch.id)
      .then((latest) => {
        if (selectedBatch.value && latest) {
          selectedBatch.value.state = latest.state
        }
        return isTerminalState(latest.state)
      })
      .catch(() => false)
  }

  function tailTick(): void {
    if (!logDrawerVisible.value || !logFollowing.value) {
      return
    }
    drainLogs()
      .then(() => refreshBatchState())
      .then((terminal) => {
        if (!logDrawerVisible.value || !logFollowing.value) {
          return
        }
        if (terminal) {
          // Drain any trailing lines flushed around completion, then stop.
          drainLogs().finally(() => stopFollowing())
        } else {
          logTimer = setTimeout(tailTick, LOG_POLL_INTERVAL_MS)
        }
      })
      .catch((error) => {
        ElMessage({
          message: getErrorMessage(error, t('message.get_batch_log_failed')),
          type: 'error'
        })
        stopFollowing()
      })
  }

  function openLog(batch: IBatch): void {
    logEpoch += 1
    if (logTimer) {
      clearTimeout(logTimer)
      logTimer = undefined
    }
    selectedBatch.value = { ...batch }
    batchLogs.value = []
    logNextFrom.value = 0
    logTruncated.value = false
    logDrawerVisible.value = true
    logLoading.value = true
    logFollowing.value = !isTerminalState(batch.state)
    drainLogs()
      .then(() => {
        scrollLogToBottom()
        if (logFollowing.value) {
          logTimer = setTimeout(tailTick, LOG_POLL_INTERVAL_MS)
        }
      })
      .catch((error) => {
        ElMessage({
          message: getErrorMessage(error, t('message.get_batch_log_failed')),
          type: 'error'
        })
      })
      .finally(() => {
        logLoading.value = false
      })
  }

  function onFollowChange(following: boolean): void {
    if (logTimer) {
      clearTimeout(logTimer)
      logTimer = undefined
    }
    if (following) {
      tailTick()
    }
  }

  function refreshLog(): void {
    if (!selectedBatch.value || logLoading.value) {
      return
    }
    logEpoch += 1
    batchLogs.value = []
    logNextFrom.value = 0
    logTruncated.value = false
    logLoading.value = true
    drainLogs()
      .then(() => scrollLogToBottom())
      .catch((error) => {
        ElMessage({
          message: getErrorMessage(error, t('message.get_batch_log_failed')),
          type: 'error'
        })
      })
      .finally(() => {
        logLoading.value = false
      })
  }

  function onLogDrawerClose(): void {
    logEpoch += 1
    stopFollowing()
    batchLogs.value = []
    logNextFrom.value = 0
    logTruncated.value = false
    selectedBatch.value = undefined
  }

  onBeforeUnmount(() => {
    if (logTimer) {
      clearTimeout(logTimer)
      logTimer = undefined
    }
  })

  function handleDeleteBatch(batch: IBatch) {
    deleteBatch(batch.id)
      .then((res) => {
        if (res.success) {
          ElMessage({
            message: t('message.cancel_succeeded', { name: 'batch' }),
            type: 'success'
          })
        } else {
          ElMessage({
            message: res.msg || t('message.cancel_failed', { name: 'batch' }),
            type: 'error'
          })
        }
      })
      .catch((error) => {
        ElMessage({
          message: getErrorMessage(
            error,
            t('message.cancel_failed', { name: 'batch' })
          ),
          type: 'error'
        })
      })
      .finally(() => {
        getList()
      })
  }

  function init() {
    getList()
    getWebUIConfig()
      .then((config) => {
        engineUIProxyConfig.engineUIProxyEnabled = config.engineUIProxyEnabled
      })
      .catch(() => {})
  }

  init()

  defineExpose({
    getAppUI,
    engineUIProxyConfig,
    isTerminalState,
    canShowAppUI,
    openLog,
    refreshLog,
    batchLogs,
    logFollowing,
    logTruncated
  })
</script>

<style lang="scss" scoped>
  header {
    display: flex;
    justify-content: flex-end;
  }
  .filter-card {
    margin-bottom: 10px;
  }
  .batch-table {
    :deep(.el-table__header .cell) {
      white-space: nowrap;
      word-break: normal;
    }
    :deep(.cell) {
      word-break: normal;
    }
  }
  .truncate-text {
    display: inline-block;
    max-width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
    vertical-align: bottom;
    white-space: nowrap;
  }
  .pager {
    margin-top: 12px;
    display: flex;
    justify-content: flex-end;
    align-items: center;
    gap: 16px;
    color: var(--el-text-color-secondary);
    font-size: 13px;
  }
  .log-body {
    display: flex;
    flex-direction: column;
    height: 100%;
  }
  .log-toolbar {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 8px;
  }
  .log-truncated {
    color: var(--el-text-color-secondary);
    font-size: 12px;
  }
  .log-container {
    flex: 1;
    min-height: 0;
    pre {
      margin: 0;
      white-space: pre-wrap;
      word-break: break-word;
      font-family: Menlo, Monaco, Consolas, 'Courier New', monospace;
      font-size: 12px;
      line-height: 1.6;
    }
  }
</style>
