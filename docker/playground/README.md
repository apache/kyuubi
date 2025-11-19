Playground
==========

## For Users

### Setup

1. Install [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/);
2. Go to `docker/playground`, and use `docker compose up -d` to run compose services as daemon;

### Play

1. Connect using `beeline`

`docker exec -it kyuubi /opt/kyuubi/bin/beeline -u 'jdbc:hive2://0.0.0.0:10009/tpcds/tiny'`;

2. Connect using DBeaver

Add a Kyuubi datasource with

- connection url `jdbc:hive2://0.0.0.0:10009/tpcds/tiny`
- username: `anonymous`
- password: `<empty>`

3. Use built-in dataset

Kyuubi supply some built-in dataset, after Kyuubi started, you can run the following command to load the different datasets:

- For loading TPC-DS tiny dataset to `spark_catalog.tpcds_tiny`, run `docker exec -it kyuubi /opt/kyuubi/bin/beeline -u 'jdbc:hive2://0.0.0.0:10009/' -f /opt/load_data/load-dataset-tpcds-tiny.sql`
- For loading TPC-H  tiny dataset to `spark_catalog.tpch_tiny`,  run `docker exec -it kyuubi /opt/kyuubi/bin/beeline -u 'jdbc:hive2://0.0.0.0:10009/' -f /opt/load_data/load-dataset-tpch-tiny.sql`

### Access Service

- RustFS: http://localhost:9001
- PostgreSQL localhost:5432 (username: postgres, password: postgres)
- Spark UI: http://localhost:4040 (available after Spark application launching by Kyuubi, port may be 4041, 4042... if you launch more than one Spark applications)
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (username: admin, password: admin)

### Shutdown

1. Stop compose services by `docker compose down`;

## For Maintainers

### Build

1. Build images `docker/playground/build-image.sh`;
2. Optional to use `buildx` to build and publish cross-platform images `BUILDX=1 docker/playground/build-image.sh`;

