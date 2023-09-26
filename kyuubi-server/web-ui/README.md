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

Notice:

Before you start the Web UI project, please make sure the Kyuubi server has been started.

Kyuubi Web UI will proxy the requests to Kyuubi server,  with the default endpoint path to`http://localhost:10099`. Modify `VITE_APP_DEV_WEB_URL` in `.env.development` for customizing targeted endpoint path.

#### Why proxy to http://localhost:10099

Currently kyuubi server binds on `http://0.0.0.0:10099` in case your are running kyuubi server in MacOS or Windows(If in linux, you should config kyuubi server `kyuubi.frontend.rest.bind.host=0.0.0.0`, or change `VITE_APP_DEV_WEB_URL` in `.env.development`).

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

If you want to save disk space and boost installation speed, we recommend using `pnpm 8.x.x` to instead of npm.
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

