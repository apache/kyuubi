Playground
===

## For Users

### Setup

1. Install [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/);
2. Start compose services `cd docker/playground && docker compose up`;

### Play

1. Connect using `beeline`

`docker exec -it kyuubi /opt/kyuubi/bin/beeline -u 'jdbc:hive2://0.0.0.0:10009/'`;

2. Connect using DBeaver

Add a Kyuubi datasource with

- connection url `jdbc:hive2://0.0.0.0:10009/`
- username: `anonymous`
- password: `<empty>`

### Access Service

- MinIO: http://localhost:9001
- PostgreSQL localhost:5432 (username: postgres, password: postgres)
- Spark UI: http://localhost:4040 (available after Spark application launching by Kyuubi, port may be 4041, 4042... if you launch more than one Spark applications)

### Shutdown

1. Stop the compose services by pressing `CTRL+C`; 
2. Remove the stopped containers `docker compose rm`;

## For Maintainers

### Build

1. Build images `docker/playground/build-image.sh`;
2. Optional to use `buildx` to build and publish cross-platform images `BUILDX=1 docker/playground/build-image.sh`;
