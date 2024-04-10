<template>
  <el-dialog v-model="dialogVisible" :close-on-click-modal="false" width="600px">
    <div class="dialog-header">
      <img class="logo" src="@/assets/images/kyuubi-logo.svg" />
    </div>
    <el-form class="login-form">
      <el-form-item label="Username">
        <el-input v-model="username" placeholder="Username" />
      </el-form-item>
      <el-form-item label="Password">
        <el-input type="password" v-model="password" placeholder="Password" />
      </el-form-item>
      <el-form-item>
        <p v-if="loginError" class="login-error">{{ loginError }}</p>
      </el-form-item>
    </el-form>
    <div slot="footer" class="dialog-footer">
      <el-button @click="closeDialog">Cancel</el-button>
      <el-button type="primary" @click="handleLogin">Login</el-button>
    </div>
  </el-dialog>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue';
import { useAuthStore } from '@/pinia/auth/auth';

const authStore = useAuthStore();
const dialogVisible = ref(false);
const username = ref('');
const password = ref('');
const loginError = ref('');

const handleLogin = async () => {
  try {
    await authStore.setUser(username.value, password.value);
    dialogVisible.value = false;
    loginError.value = '';
  } catch (error) {
    loginError.value = (error as Error).message;
  }
};

const closeDialog = () => {
  dialogVisible.value = false;
  loginError.value = '';
};

onMounted(() => {
  window.addEventListener('auth-required', () => {
    dialogVisible.value = true;
  });
});
</script>

<style scoped>
.dialog-header {
  display: flex;
  flex-direction: column;
  align-items: center;
  margin-bottom: 20px;
}

.logo {
  width: 80px;
  height: auto;
  margin-bottom: 10px;
}

.login-form {
  margin-bottom: 20px;
}

.login-error {
  color: red;
  margin-top: 10px;
  text-align: center;
}

.dialog-footer {
  text-align: right;
  padding: 15px 20px;

  &>button:not(:last-child) {
    margin-right: 10px;
  }
}
</style>
