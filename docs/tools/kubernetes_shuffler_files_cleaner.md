<div align=center>

![](../imgs/kyuubi_logo.png)

</div>

# Kubernetes tools shuffler-files-cleaner

## Requirements
You'd better have cognition upon the following things when you want to use shuffle-files-cleaner

* Read this article
* An active Kubernetes cluster
* [Kubectl](https://kubernetes.io/docs/reference/kubectl/overview/)

## Purpose

When running Spark On Kubernetes, we encountered such a situation that after using hostPath volume local-dir, the usage rate of the directory storing shuffle files remained high.

So an additional tool is needed to clear the accumulated shuffle files.

## Usage

### build Shuffler Cleaner Docker Image
```shell
    docker build ./tools/kubernetes/docker/shuffler-files-cleaner/Dockerfile
```

### modify cleaner.yml
You need to modify the cleaner.yml to fit your current environment.

```yaml
volumes:
  # Directory on the host which store shuffle dirs
  - name: shuffle-files-dirs
    hostPath:
      path: /shuffleFilesDirs
  # Directory on the host which you want to store clean log
  - name: cleaner-log
    hostPath:
      path: /logDir
```

The above parameters used to help volume hostPath to container path.

Then you should modify following parameter to fit environment.

The `cacheDirs` value should fit your actual situation.

```yaml
env:
  # Set env to manager cleaner running
  # the target dirs which in container
  - name: cacheDirs
    value: /data/data1,/data/data2
```

Such as you use /shuffleFilesDirs/data1, /shuffleFilesDirs/data2 to cache shuffle data.

You should modify 
```yaml
volumes:
  - name: shuffle-files-dirs
    hostPath:
      path: /shuffleFilesDirs
```

And then

```yaml
env:
  # Set env to manager cleaner running
  # the target dirs which in container
  - name: cacheDirs
    value: /data/data1,/data/data2
```

Also, you can change the prefix /data by modify following parameter
```yaml
volumeMounts:
  - name: shuffle-files-dirs
    ountPath: /data
```
### start daemonSet

After you finishing modifying the above, you can use the following command `kubectl apply -f cleaner.yml` to start daemonSet.

## Related parameters

Name | Default | Meaning
--- | --- | --- |
cacheDirs | /data/data1,/data/data2 | The target dirs in container path which will clean shuffle files.
fileExpiredTime | 604800000 | Cleaner will clean the shuffle files which current time - last modified time  more than the fileExpiredTime.
deepCleanFileExpiredTime | 432000000 | Deep clean will clean the shuffle files which current time - last modified time  more than the deepCleanFileExpiredTime.
freeSpaceThreshold | 60 | After first clean, if free Space low than threshold trigger deep clean.
sleepTime | 3600000 | Cleaner sleep between cleaning.