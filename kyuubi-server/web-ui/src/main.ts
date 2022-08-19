import { createApp } from 'vue'
import router from '@/router'
import { store } from '@/pinia'
import i18n from '@/locales'
import ElementPlus from 'element-plus'
import '@/styles/element/index.scss'
import './styles/index.scss'
import App from './App.vue'
import * as ElementPlusIconsVue from '@element-plus/icons-vue'

const app = createApp(App)
for (const [key, component] of Object.entries(ElementPlusIconsVue)) {
  app.component(key, component)
}
app.use(router).use(store).use(i18n).use(ElementPlus).mount('#app')
