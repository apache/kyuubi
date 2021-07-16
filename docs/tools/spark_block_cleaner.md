<div align=center>

![](../imgs/kyuubi_logo.png)

</div>

# Kubernetes tools spark-block-cleaner

## Requirements
You'd better have cognition upon the following things when you want to use spark-block-cleaner.

* Read this article
* An active Kubernetes cluster
* [Kubectl](https://kubernetes.io/docs/reference/kubectl/overview/)

## Purpose

When running Spark On Kubernetes, we encountered such a situation that after using hostPath volume local-dir, the usage rate of the directory storing shuffle files remained high.

So an additional tool is needed to clear the accumulated block files.

## Usage

### Build Block Cleaner Docker Image
```shell
    docker build ./tools/spark-block-cleaner/kubernetes/docker
```

### Modify spark-block-cleaner.yml
You need to modify the spark-block-cleaner.yml to fit your current environment.

```yaml
volumes:
  # Directory on the host which store block dirs
  - name: block-files-dir-1
    hostPath:
      path: /blockFilesDirs/data1
  - name: block-files-dir-2
    hostPath:
      path: /blockFilesDirs/data2
  # Directory on the host which you want to store clean log
  - name: cleaner-log
    hostPath:
      path: /logDir
```

The above parameters used to help volume hostPath to container path.

Then you should modify following parameter to fit environment.

The `CACHE_DIRS` value should fit your actual situation.

```yaml
env:
  # Set env to manager cleaner running
  # the target dirs which in container
  - name: CACHE_DIRS
    value: /data/data1,/data/data2
```

Such as you used /blockFilesDirs/data1, /blockFilesDirs/data2 as spark-local-dirs to cache shuffle data.

You should modify
```yaml
volumes:
  - name: block-files-dir-1
    hostPath:
      path: /blockFilesDirs/data1
  - name: block-files-dir-2
    hostPath:
      path: /blockFilesDirs/data2
```

And then

```yaml
env:
  # Set env to manager cleaner running
  # the target dirs which in container
  - name: CACHE_DIRS
    value: /data/data1,/data/data2
```
You can modify the following parameters in spark-block-cleaner.yml containers envs to control spark-block-cleaner running.

### Start daemonSet

After you finishing modifying the above, you can use the following command `kubectl apply -f spark-block-cleaner.yml` to start daemonSet.

## Related parameters

Name | Default | Meaning
--- | --- | ---
CACHE_DIRS | /data/data1,/data/data2 | The target dirs in container path which will clean block files.
FILE_EXPIRED_TIME | 604800 | Cleaner will clean the block files which current time - last modified time  more than the fileExpiredTime.
DEEP_CLEAN_FILE_EXPIRED_TIME | 432000 | Deep clean will clean the block files which current time - last modified time  more than the deepCleanFileExpiredTime.
FREE_SPACE_THRESHOLD | 60 | After first clean, if free Space low than threshold trigger deep clean.
SCHEDULE_INTERVAL | 3600 | Cleaner sleep between cleaning.
