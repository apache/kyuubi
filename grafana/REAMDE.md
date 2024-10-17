# How to enable a grafana dashboard for Kyuubi

## Overview

Grafana is introduced to visualize and monitor metrics from Kyuubi service collected by Prometheus.

There are two ways for users to make grafana dashboard available.

## for environment that has grafana installed

1. Click on the import button on the left side of grafana home page
2. Import the dashboard json file named dashboard_template under grafana folder

## for environment without grafana built

Here we provide a solution for users to easily build a grafana image with desired dashboards.

Data sources and dashboards both are defined and managed via yml files. user can add or remove data sources and
dashboards based on their needs.

The default data source is prometheus and its corresponding dashboard is predefined in dashboard_template json file.

Below is the command to build a grafana image with grafana dashboards(json format files) that are predefined under the
grafana folder.

Once the grafana starts up, it will load all datasource and dashboards as per two config files(dashboard.yml and
datasource.yml)

```
1. docker build --build-arg PROMETHEUS_URL_ARG="127.0.0.1:8080" -t grafana:kyuubi -f Dockerfile .

Options:

--build-arg PROMETHEUS_URL_ARG: the url to access promethues datasource

-t: the target repo and tag name for image

-f: this docker file

```