const routes = [
  {
    path: '/operation/runningJobs',
    name: 'operation-runningJobs',
    component: () => import('@/views/operation/runningJobs/index.vue'),
  },
  {
    path: '/operation/completedJobs',
    name: 'operation-completedJobs',
    component: () => import('@/views/operation/completedJobs/index.vue'),
  },
]

export default routes
