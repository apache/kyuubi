# Kyuubi Grafana Dashboard

[Grafana](https://grafana.com/) is a popular open and composable observability platform. Kyuubi provides
a Grafana Dashboard template `dashboard-template.json` to help users to monitor the Kyuubi server.

## For Users

By default, Kyuubi server enables metrics system and exposes Prometheus endpoints at `http://<host>:10019/metrics`,
to use the Kyuubi Grafana Dashboard, you are supposed to have an available Prometheus and Grafana service, then
configure Prometheus to scrape Kyuubi metrics, add the Prometheus data source into Grafana, and then import the
`dashboard-template.json` into Grafana and customize. For more details, please read the
[Kyuubi Docs](https://kyuubi.readthedocs.io/en/master/monitor/metrics.html#grafana-and-prometheus)

## For Developers

If you have good ideas to improve the dashboard, please don't hesitate to reach out to us by opening
GitHub [Issues](https://github.com/apache/kyuubi/issues)/[PRs](https://github.com/apache/kyuubi/pulls)
or sending an email to `dev@kyuubi.apache.org`.

### Export Grafana Dashboard template

Depends on your Grafana version, the exporting steps might be a little different.

Use Grafana 11.4 as an example, after modifying the dashboard, save your changes and click the "Share" button
on the top-right corner, then choose the "Export" tab and enable the "Export for sharing externally", finally,
click the "View JSON" button and update the `dashboard-template.json` with that JSON content.

We encourage the developers to use a similar version of Grafana to the existing `dashboard-template.json`,
and focus on one topic in each PR, to avoid introducing unnecessary and huge diff of `dashboard-template.json`.
Additionally, to make the reviewers easy to understand your changes, don't forget to attach the current and
updated dashboard screenshots in your PR description.
