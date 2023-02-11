# Kyuubi Web UI

### Start Using

For the best experience, we recommend using `node 16.x.x`.
You can learn how to install the corresponding version from its official website.

- [node](https://nodejs.org/en/)

### Install Dependencies

```shell
npm install
```

### Development Project

To do this you can change the VITE_APP_DEV_WEB_URL parameter variable as the service url in `.env.development` in the project root directory, such as http://127.0. 0.1:8090

```shell
npm run dev
```

### Build Project

```shell
npm run build
```

### Code Format

Usually after you modify the code, you need to perform code formatting operations to ensure that the code in the project is the same style.

```shell
npm run prettier
```

### Recommend

If you want to save disk space and boost installation speed, we recommend using `pnpm 7.x.x` to instead of npm.
You can learn how to install the corresponding version from its official website.

- [pnpm](https://pnpm.io/)

```shell
# Install Dependencies
pnpm install

# Development Project
pnpm run dev

# Build Project
pnpm run build

# Code Format
pnpm run prettier
```

