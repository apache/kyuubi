# Trouble Shooting

## Debugging Kyuubi
```shell script
KYUUBI_JAVA_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 bin/kyuubi.sh start
```