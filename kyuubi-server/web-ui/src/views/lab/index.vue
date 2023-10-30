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
  <div class="container">
    <el-tabs
      v-model="editableTabsValue"
      type="card"
      class="sql-lab-tabs"
      editable
      @edit="handleTabsEdit">
      <el-tab-pane
        v-for="item in editableTabs"
        :key="item.name"
        :label="item.title"
        :name="item.name">
        <Editor />
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script lang="ts" setup>
  import Editor from './components/Editor.vue'
  import { ref } from 'vue'
  import { TabPaneName } from 'element-plus'

  const editableTabsValue = ref('0')
  const editableTabs = ref([
    {
      title: 'Sql',
      name: '0'
    }
  ])

  const handleTabsEdit = (
    targetName: TabPaneName | undefined,
    action: 'remove' | 'add'
  ) => {
    if (action === 'add') {
      const tabLength = editableTabs.value.length
      const newTabName = `${tabLength}`
      editableTabs.value.push({
        title: `Sql${tabLength}`,
        name: newTabName
      })
      editableTabsValue.value = newTabName
    } else if (action === 'remove') {
      const tabs = editableTabs.value
      let activeName = editableTabsValue.value
      if (activeName === targetName) {
        tabs.forEach((tab, index) => {
          if (tab.name === targetName) {
            const nextTab = tabs[index + 1] || tabs[index - 1]
            if (nextTab) {
              activeName = nextTab.name
            }
          }
        })
      }

      editableTabsValue.value = activeName
      editableTabs.value = tabs.filter((tab) => tab.name !== targetName)
    }
  }
</script>

<style lang="scss" scoped>
  .container {
    height: 70%;

    :deep(.sql-lab-tabs) {
      height: 100%;
      .el-tabs__header {
        display: flex;
        flex-direction: row-reverse;
        justify-content: flex-end;
      }

      /* 自定义选项卡面板的高度 */
      .el-tabs__content {
        height: 300px; /* 你可以根据需要设置高度 */
        overflow: auto; /* 如果内容溢出，可以启用滚动条 */

        .el-tab-pane {
          height: 100%;
        }
      }
    }
  }
</style>
