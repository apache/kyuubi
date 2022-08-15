<template>
  <header>
    <img src="@/assets/kyuubi.png" />
    <article v-if="!isCollapse">Apache Kyuubi Dashboard</article>
  </header>
  <c-menu :is-collapse="isCollapse" :active-path="activePath" :menus="menus" />
</template>

<script setup lang="ts">
  import { ref, reactive } from 'vue'
  import { useStore } from '@/pinia/layout'
  import { storeToRefs } from 'pinia'
  import { useRoute } from 'vue-router'
  import { MENUS } from './types'
  import cMenu from '@/components/menu/index.vue'

  const menus = reactive(MENUS)
  const store = useStore()
  const { isCollapse } = storeToRefs(store)
  const router = useRoute()
  const activePath = ref(router.path)
</script>

<style lang="scss" scoped>
  header {
    position: absolute;
    top: 0;
    left: 0;
    height: 64px;
    padding-left: 16px;
    line-height: 64px;
    img {
      display: inline-block;
      width: 32px;
      height: 32px;
      position: relative;
      top: -4px;
      vertical-align: middle;
    }
    article {
      display: inline-block;
      margin-left: 12px;
      color: #fff;
      font-weight: 600;
      font-size: 14px;
      font-family: 'Myriad Pro', 'Helvetica Neue', Arial, Helvetica, sans-serif;
    }
  }
  .el-menu {
    margin-top: 64px;
  }
</style>
