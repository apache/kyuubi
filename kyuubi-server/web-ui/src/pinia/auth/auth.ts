import { defineStore } from 'pinia';
import request from '@/utils/request';

export const useAuthStore = defineStore('auth', {
  state: () => ({
    user: null as string | null,
    authToken: null as string | null,
    isAuthenticated: false,
  }),
  actions: {
    async setUser(user: string, password: string) {
      try {
        const response = await request({
          url: 'api/v1/ping',
          method: 'get',
          auth: {
            username: user,
            password: password,
          },
        });

        if (response) {
          this.user = user;
          this.authToken = `Basic ${btoa(user + ':' + password)}`;
          this.isAuthenticated = true;
        } else {
          throw new Error('Authentication failed');
        }
      } catch (error) {
        throw error;
      }
    },
    clearUser() {
      this.user = null;
      this.authToken = null;
      this.isAuthenticated = false;
    },
  },
  persist: {
    key: 'auth',
  },
});
