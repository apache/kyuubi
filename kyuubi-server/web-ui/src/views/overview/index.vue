<template>
  <main>
    <el-row :gutter="12">
      <el-col v-for="(card, index) in cards" :key="index" :md="8">
        <c-card :title="card.title" :value="card.value" />
      </el-col>
    </el-row>
  </main>
</template>

<script lang="ts" setup>
  import { reactive } from 'vue'
  import cCard from '@/components/card/index.vue'

  const cards = reactive([
    {
      title: 'Opened Session',
      value: 1,
    },
    {
      title: 'ExecPool Size',
      value: 2,
    },
    {
      title: 'ExecPool ActiveCount',
      value: 3,
    },
  ])
</script>

<style scoped></style>
