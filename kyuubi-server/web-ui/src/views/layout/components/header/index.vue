<template>
  <div class="header-container">
    <el-icon :size="20" @click="_changeCollapse">
      <component :is="isCollapse ? 'Expand' : 'Fold'" />
    </el-icon>
    <el-dropdown @command="handleClick">
      <span class="el-dropdown-link">
        {{ currentLocale }}
        <el-icon class="el-icon--right">
          <arrow-down />
        </el-icon>
      </span>
      <template #dropdown>
        <el-dropdown-menu>
          <el-dropdown-item
            v-for="(locale, key) in locales"
            :key="key"
            :command="locale.key"
            >{{ locale.label }}</el-dropdown-item
          >
        </el-dropdown-menu>
      </template>
    </el-dropdown>
  </div>
</template>

<script lang="ts" setup>
  import { useStore } from '@/pinia/layout'
  import { storeToRefs } from 'pinia'
  import { useLocales } from './use-locales'
  import { LOCALES } from './types'
  import { reactive } from 'vue'

  const locales = reactive(LOCALES)
  const { changeLocale, currentLocale } = useLocales()
  const store = useStore()
  const { isCollapse } = storeToRefs(store)
  const { changeCollapse } = store

  function _changeCollapse() {
    changeCollapse()
  }

  function handleClick(command: string) {
    changeLocale(command)
  }
</script>

<style lang="scss" scoped>
  .header-container {
    display: flex;
    justify-content: space-between;
    width: 100%;

    > .el-icon {
      padding: 0 24px;
      cursor: pointer;
    }

    > .el-dropdown .el-icon {
      position: relative;
      top: 2px;
    }
  }
</style>
