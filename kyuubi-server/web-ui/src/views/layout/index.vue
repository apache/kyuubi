<template>
  <div class="common-layout">
    <el-container>
      <el-aside>
        <Aside />
      </el-aside>
      <el-container>
        <el-header>
          <Header />
        </el-header>
        <el-main>
          <router-view />
        </el-main>
      </el-container>
    </el-container>
  </div>
</template>

<script setup lang="ts">
  import { onMounted } from 'vue'
  import Aside from './components/aside/index.vue'
  import Header from './components/header/index.vue'
  import { useLocalesStore } from '@/pinia/locales/locales'
  import { useI18n } from 'vue-i18n'

  const { locale } = useI18n()
  const localesStore = useLocalesStore()

  onMounted(() => {
    locale.value = localesStore.getLocale
  })
</script>

<style lang="scss" scoped>
  .common-layout {
    height: 100%;

    .el-container {
      min-height: 100vh;

      ::v-deep(.el-aside) {
        width: auto;
        position: relative;
        background: #001529;
      }

      .el-header {
        display: flex;
        align-items: center;
        height: 64px;
        padding: 0 12px 0 0;
        border-bottom: 1px solid #f0f0f0;
        background: #fff;
        box-shadow: 0 1px 4px rgb(0 21 41 / 8%);
      }
    }
  }
</style>
