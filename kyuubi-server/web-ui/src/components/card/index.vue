<template>
  <el-card :shadow="shadow">
    <header>{{ title }}</header>
    <main>{{ value }}</main>
  </el-card>
</template>

<script lang="ts">
  export default {
    name: 'CardIndex',
    props: {
      title: {
        type: [String, Number],
        required: true,
      },
      value: {
        type: [String, Number, Boolean, Date],
        required: true,
      },
      shadow: {
        type: String,
        default: 'always',
      },
    },
  }
</script>

<style lang="scss" scoped>
  main {
    color: #52c41a;
    font-size: 36px;
    line-height: 38px;
  }
</style>
