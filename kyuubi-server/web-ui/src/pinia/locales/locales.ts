import { defineStore } from 'pinia'
import { LocaleStore, Locale } from './types'

export const useLocalesStore = defineStore({
  id: 'locale',
  state: (): LocaleStore => ({
    locale: 'en_US',
  }),
  persist: true,
  getters: {
    getLocale(): Locale {
      return this.locale
    },
  },
  actions: {
    setLocale(lang: Locale): void {
      this.locale = lang
    },
  },
})
