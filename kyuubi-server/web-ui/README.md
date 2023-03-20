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

You should start your kyuubi server first, and then start the web ui project.

Kyuubi Web UI will proxy the request to kyuubi server, in default the proxy target is `http://localhost:10099`, you can modify `VITE_APP_DEV_WEB_URL` in `.env.development` to proxy to another url.

#### Why proxy to http://localhost:10099

Currently kyuubi server will bind on `http://0.0.0.0:10099` in case your are running kyuubi server in MacOS or Windows(If in linux, you should config kyuubi server `kyuubi.frontend.rest.bind.host=0.0.0.0`).

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

