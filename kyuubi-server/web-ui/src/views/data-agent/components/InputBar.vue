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
  <div class="input-bar">
    <div class="input-card">
      <el-input
        ref="inputRef"
        v-model="inputText"
        type="textarea"
        :rows="3"
        :autosize="{ minRows: 3, maxRows: 8 }"
        :placeholder="placeholder || 'Type your question...'"
        :disabled="disabled"
        resize="none"
        @keydown="handleKeydown" />
      <div class="input-actions">
        <span class="char-count" :class="{ 'is-long': inputText.length > 500 }">
          {{ inputText.length > 0 ? inputText.length : '' }}
        </span>
        <el-button
          class="send-btn"
          type="primary"
          :disabled="disabled || !inputText.trim()"
          @click="send">
          <el-icon :size="18"><Promotion /></el-icon>
        </el-button>
      </div>
    </div>
    <div class="input-hint">
      <kbd>Enter</kbd> {{ $t('data_agent.input_hint_send') }} &middot;
      <kbd>Shift+Enter</kbd> {{ $t('data_agent.input_hint_newline') }}
    </div>
  </div>
</template>

<script lang="ts" setup>
  import { ref } from 'vue'
  import { Promotion } from '@element-plus/icons-vue'

  const props = defineProps<{
    disabled: boolean
    placeholder?: string
  }>()

  const emit = defineEmits<{
    (e: 'send', text: string): void
  }>()

  const inputText = ref('')
  const inputRef = ref()

  function handleKeydown(e: KeyboardEvent) {
    if (e.key === 'Enter' && !e.shiftKey && !e.isComposing) {
      e.preventDefault()
      send()
    }
  }

  function send() {
    const text = inputText.value.trim()
    if (!text || props.disabled) return
    emit('send', text)
    inputText.value = ''
  }
</script>

<style lang="scss" scoped>
  .input-bar {
    padding: 0 10% 16px;
    background: #f7f8fa;
    flex-shrink: 0;
  }

  .input-card {
    display: flex;
    align-items: flex-end;
    gap: 10px;
    background: #fff;
    border: 1.5px solid #e5e6eb;
    border-radius: 18px;
    padding: 14px 14px 14px 6px;
    box-shadow: 0 4px 20px rgba(0, 0, 0, 0.07);
    transition: border-color 0.2s, box-shadow 0.2s;

    &:focus-within {
      border-color: #409eff;
      box-shadow: 0 4px 24px rgba(64, 158, 255, 0.18);
    }

    :deep(.el-textarea__inner) {
      border: none;
      box-shadow: none;
      padding: 4px 12px;
      font-size: 15px;
      line-height: 1.6;
      background: transparent;

      &:focus {
        box-shadow: none;
      }
    }
  }

  .input-actions {
    display: flex;
    align-items: flex-end;
    gap: 10px;
    flex-shrink: 0;
    padding-bottom: 2px;
  }

  .char-count {
    font-size: 11px;
    color: #c0c4cc;
    min-width: 24px;
    text-align: right;
    transition: color 0.2s;
    padding-bottom: 10px;

    &.is-long {
      color: #e6a23c;
    }
  }

  .send-btn {
    width: 42px;
    height: 42px;
    border-radius: 12px;
    padding: 0;
    flex-shrink: 0;
  }

  .input-hint {
    text-align: center;
    font-size: 11px;
    color: #c0c4cc;
    margin-top: 8px;

    kbd {
      background: #fff;
      border: 1px solid #e5e6eb;
      border-radius: 3px;
      padding: 1px 5px;
      font-size: 10px;
      font-family: inherit;
      color: #86909c;
    }
  }
</style>
