import { ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { useLocalesStore } from '@/pinia/locales/locales'
import type { Locale } from '@/pinia/locales/types'
import { LOCALES } from './types'

export function useLocales() {
  const { locale } = useI18n()
  const localesStore = useLocalesStore()

  const currentLocale = ref(
    LOCALES.find((item: { key: string }) => item.key === localesStore.getLocale)
      ?.label || 'English'
  )

  const changeLocale = (key: string | number) => {
    localesStore.setLocale((locale.value = key as Locale))
    currentLocale.value = LOCALES.find(
      (item: { key: string }) => item.key === key
    )?.label as string
  }
  return {
    changeLocale,
    currentLocale,
  }
}
