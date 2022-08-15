import { createRouter, createWebHistory } from 'vue-router'
import overviewRoutes from './overview'
import workloadRoutes from './workload'
import operationRoutes from './operation'
import contactRoutes from './contact'

const routes = [
  {
    path: '/',
    name: 'main',
    redirect: {
      name: 'layout',
    },
  },
  {
    path: '/layout',
    name: 'layout',
    component: () => import('@/views/layout/index.vue'),
    redirect: 'overview',
    children: [
      ...overviewRoutes,
      ...workloadRoutes,
      ...operationRoutes,
      ...contactRoutes,
    ],
  },
]

const router = createRouter({
  history: createWebHistory(),
  routes,
})

export default router
