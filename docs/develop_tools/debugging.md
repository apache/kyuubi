<div align=center>

![](../imgs/kyuubi_logo.png)

</div>

# Debugging Kyuubi

You can use the [Java Debug Wire Protocol](https://docs.oracle.com/javase/8/docs/technotes/guides/jpda/conninv.html#Plugin) to debug Kyuubi
with your favorite IDE tool, e.g. Intellij IDEA.

## Debugging Server

We can configure the JDWP agent in `KYUUBI_JAVA_OPTS` for debugging.
 
 
For example,
```bash
KYUUBI_JAVA_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 \
bin/kyuubi start
```

In the IDE, you set the corresponding parameters(host&port) in debug configurations, for example,
<div align=center>

![](../imgs/idea_debug.png)

</div>

## Debugging Apps

- Spark Driver

```bash
spark.driver.extraJavaOptions   -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005
```

- Spark Executor
```bash
spark.executor.extraJavaOptions   -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005
```