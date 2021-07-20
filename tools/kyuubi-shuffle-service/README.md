# Kyuubi Spark External Shuffle Service

This enables the missing feature of Dynamic Resource Allocation(a.k.a, DRA, DA) for running Spark on Kubernetes, which provides the auto-scaling capability for Spark application, especially downscaling.

Although there is a feature called shuffle tracking to track the shuffle data and tell Spark to remove executor while the data exceeds the time to live, these removals are coarse-grained.


## Prerequisites

- Every thing you need to know about Spark on Kubernetes
- Running Spark on Kubernetes apps with hostNetwork enabled
- Running this module as a DaemonSet with hostNetwork enabled
- Self-managed shuffle data cleaner as the state of apps will not be posted to this service
  - See: tools/spark-block-cleaner/kubernetes/spark-block-cleaner.yml


## Usage

Check `tools/kyuubi-shuffle-service/kubernetes/example.yaml` for more information.
