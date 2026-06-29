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
  <aside class="sessions-rail" :class="{ collapsed: railCollapsed }">
    <div class="rail-head">
      <span v-if="!railCollapsed" class="rail-title">{{
        $t('data_agent.conversations')
      }}</span>
      <div class="rail-head-actions">
        <button
          v-if="!railCollapsed"
          class="rail-icon-btn"
          :title="$t('data_agent.new_conversation')"
          @click="emit('new')">
          <el-icon :size="14"><Plus /></el-icon>
        </button>
        <button
          class="rail-icon-btn"
          :title="
            railCollapsed
              ? $t('data_agent.expand_rail')
              : $t('data_agent.collapse_rail')
          "
          @click="railCollapsed = !railCollapsed">
          <el-icon :size="14">
            <component :is="railCollapsed ? Expand : Fold" />
          </el-icon>
        </button>
      </div>
    </div>
    <button
      v-if="railCollapsed"
      class="rail-icon-btn rail-collapsed-new"
      :title="$t('data_agent.new_conversation')"
      @click="emit('new')">
      <el-icon :size="14"><Plus /></el-icon>
    </button>
    <div class="rail-list">
      <div
        v-for="s in sessions"
        :key="s.id"
        class="rail-item"
        :class="{ active: s.id === activeSessionId }"
        :title="s.title || $t('data_agent.untitled_session')"
        @click="emit('select', s.id)">
        <span
          class="rail-item-status"
          :class="{
            streaming: s.streaming,
            error: !s.streaming && !!s.errorMessage,
            active: s.id === activeSessionId
          }" />
        <div v-if="!railCollapsed" class="rail-item-text">
          <input
            v-if="editingId === s.id"
            ref="renameInputs"
            v-model="editingTitle"
            class="rail-item-title-input"
            :maxlength="64"
            @click.stop
            @keydown.enter.prevent="commitRename"
            @keydown.esc.prevent="cancelRename"
            @blur="commitRename" />
          <div v-else class="rail-item-title" @dblclick.stop="beginRename(s)">
            {{ s.title || $t('data_agent.untitled_session') }}
          </div>
        </div>
        <div
          v-if="!railCollapsed && editingId !== s.id"
          class="rail-item-actions">
          <button
            class="rail-icon-btn"
            :title="$t('data_agent.rename_conversation')"
            @click.stop="beginRename(s)">
            <el-icon :size="12"><EditPen /></el-icon>
          </button>
          <button
            v-if="sessions.length > 1"
            class="rail-icon-btn danger"
            :title="$t('data_agent.close_conversation')"
            @click.stop="confirmClose(s)">
            <el-icon :size="12"><Close /></el-icon>
          </button>
        </div>
      </div>
    </div>
  </aside>
</template>

<script lang="ts" setup>
  import { ref } from 'vue'
  import { ElMessageBox } from 'element-plus'
  import { useI18n } from 'vue-i18n'
  import { Plus, Fold, Expand, EditPen, Close } from '@element-plus/icons-vue'
  import { useDataAgentStore, type DataAgentSession } from '@/pinia/data-agent'

  defineProps<{
    sessions: DataAgentSession[]
    activeSessionId: string
  }>()

  const emit = defineEmits<{
    (e: 'select', id: string): void
    (e: 'new'): void
    (e: 'close', id: string): void
  }>()

  const store = useDataAgentStore()
  const { t } = useI18n()
  const railCollapsed = ref(false)

  async function confirmClose(s: DataAgentSession) {
    const title = s.title || t('data_agent.untitled_session')
    try {
      await ElMessageBox.confirm(
        t('data_agent.close_conversation_confirm', { title }),
        t('data_agent.close_conversation'),
        {
          confirmButtonText: t('data_agent.close_conversation_confirm_ok'),
          cancelButtonText: t('data_agent.close_conversation_confirm_cancel'),
          type: 'warning',
          autofocus: false
        }
      )
    } catch {
      return
    }
    emit('close', s.id)
  }

  const editingId = ref('')
  const editingTitle = ref('')
  const renameInputs = ref<HTMLInputElement[] | HTMLInputElement | null>(null)

  function beginRename(s: DataAgentSession) {
    editingId.value = s.id
    editingTitle.value = s.title
    setTimeout(() => {
      const el = Array.isArray(renameInputs.value)
        ? renameInputs.value[0]
        : renameInputs.value
      el?.focus()
      el?.select()
    }, 0)
  }

  function commitRename() {
    if (!editingId.value) return
    store.renameSession(editingId.value, editingTitle.value)
    editingId.value = ''
    editingTitle.value = ''
  }

  function cancelRename() {
    editingId.value = ''
    editingTitle.value = ''
  }
</script>

<style lang="scss" scoped>
  .sessions-rail {
    display: flex;
    flex-direction: column;
    width: 240px;
    background: #fafbfc;
    border-left: 1px solid #ebeef5;
    transition: width 0.2s ease;
    flex-shrink: 0;
    order: 1;

    &.collapsed {
      width: 44px;
    }
  }
  .rail-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0 8px 0 14px;
    height: 52px;
    flex-shrink: 0;
  }
  .sessions-rail.collapsed .rail-head {
    padding: 0;
    justify-content: center;
  }
  .rail-title {
    font-size: 12px;
    font-weight: 600;
    color: #86909c;
    text-transform: uppercase;
    letter-spacing: 0.6px;
  }
  .rail-head-actions {
    display: flex;
    align-items: center;
    gap: 2px;
  }
  .rail-collapsed-new {
    align-self: center;
    margin-bottom: 6px;
  }
  .rail-icon-btn {
    width: 26px;
    height: 26px;
    border: none;
    border-radius: 6px;
    background: transparent;
    color: #909399;
    cursor: pointer;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    transition: background 0.15s, color 0.15s;
    padding: 0;

    &:hover {
      background: #eef0f3;
      color: #303133;
    }
    &.danger:hover {
      background: #fff0ef;
      color: #f56c6c;
    }
  }
  .rail-list {
    flex: 1;
    overflow-y: auto;
    padding: 2px 6px 12px;

    &::-webkit-scrollbar {
      width: 4px;
    }
    &::-webkit-scrollbar-thumb {
      background: #dcdfe6;
      border-radius: 2px;
    }
  }
  .rail-item {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 7px 8px;
    border-radius: 6px;
    cursor: pointer;
    margin-bottom: 1px;
    transition: background 0.15s;

    &:hover {
      background: #eef0f3;
    }
    &.active {
      background: #fff;
      box-shadow: inset 0 0 0 1px #d9e6f5;
    }
  }
  .sessions-rail.collapsed .rail-item {
    justify-content: center;
    padding: 7px 4px;
  }
  .rail-item-status {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: #d3d7df;
    flex-shrink: 0;
    transition: background 0.2s;

    &.active {
      background: #409eff;
    }
    &.streaming {
      background: #409eff;
      animation: rail-pulse 1.2s ease-in-out infinite;
    }
    &.error {
      background: #f56c6c;
    }
  }
  @keyframes rail-pulse {
    0%,
    100% {
      opacity: 1;
    }
    50% {
      opacity: 0.35;
    }
  }
  .rail-item-text {
    flex: 1;
    min-width: 0;
  }
  .rail-item-title {
    font-size: 13px;
    color: #303133;
    line-height: 1.4;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .rail-item.active .rail-item-title {
    color: #1d2129;
    font-weight: 500;
  }
  .rail-item-title-input {
    width: 100%;
    box-sizing: border-box;
    font-size: 13px;
    color: #303133;
    border: 1px solid #409eff;
    border-radius: 4px;
    padding: 1px 6px;
    outline: none;
    background: #fff;
  }
  .rail-item-actions {
    display: none;
    align-items: center;
    gap: 0;
    flex-shrink: 0;
  }
  .rail-item:hover .rail-item-actions,
  .rail-item.active .rail-item-actions {
    display: inline-flex;
  }
  .rail-item-actions .rail-icon-btn {
    width: 22px;
    height: 22px;
  }
</style>
