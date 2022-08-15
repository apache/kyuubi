<template>
  <el-menu
    class="el-menu-container"
    :collapse="isCollapse"
    :default-active="activePath"
    :router="true"
  >
    <template v-for="(menu, index) in menus">
      <el-menu-item
        v-if="!menu.children || menu.children.length === 0"
        :key="index + '-1'"
        :index="menu.router"
      >
        <el-icon>
          <component :is="menu.icon"></component>
        </el-icon>
        <span>{{ menu.label }}</span>
      </el-menu-item>
      <el-sub-menu v-else :key="index + '-2'" :index="String(index)">
        <template #title>
          <el-icon :size="16">
            <component :is="menu.icon"></component>
          </el-icon>
          <span>{{ menu.label }}</span>
        </template>
        <el-menu-item
          v-for="(child, index2) in menu.children"
          :key="index2"
          :index="child.router"
        >
          <el-icon :size="16">
            <component :is="child.icon"></component>
          </el-icon>
          <span>{{ child.label }}</span>
        </el-menu-item>
      </el-sub-menu>
    </template>
  </el-menu>
</template>

<script lang="ts">
  export default {
    name: 'MenuIndex',
    props: {
      isCollapse: {
        type: Boolean,
        required: true,
      },
      menus: {
        type: Array,
        default: () => [],
      },
      activePath: {
        type: String,
      },
    },
  }
</script>

<style lang="scss" scoped>
  .el-menu-container {
    padding: 16px 0;
    border-right: 0;
    &:not(.el-menu--collapse) {
      width: 260px;
    }
    .el-menu-item.is-active {
      background: #1890ff;
      color: #fff;
    }
    .el-sub-menu__title,
    .el-menu-item {
      &:hover {
        > i,
        > span {
          color: #fff;
        }
      }
    }
  }
</style>
