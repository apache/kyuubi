const routes = [
  {
    path: '/workload/analysis',
    name: 'workload-analysis',
    component: () => import('@/views/workload/analysis/index.vue'),
  },
  {
    path: '/workload/queue',
    name: 'workload-queue',
    component: () => import('@/views/workload/queue/index.vue'),
  },
  {
    path: '/workload/session',
    name: 'workload-session',
    component: () => import('@/views/workload/session/index.vue'),
  },
  {
    path: '/workload/query',
    name: 'workload-query',
    component: () => import('@/views/workload/query/index.vue'),
  },
]

export default routes
