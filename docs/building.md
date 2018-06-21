# Building Kyuubi

## Building Kyuubi with Apache Maven
**Kyuubi** server is built based on [Apache Maven](http://maven.apache.org),

```bash
./build/mvn clean package -DskipTests
```

Running the code above in the Kyuubi project root directory is all we need to build a runnable Kyuubi server.

Besides, you can specify a particular maven profile of Spark to build kyuubi towards different Spark versions.

spark version| maven profile | notes
---|---|---
1.x.x|(none)| not supported
2.0.x|(none)| not supported
2.1.2|`-Pspark-2.1` | spark-2.1 is the default profile for building kyuubi and it defacto supports all 2.1.x and above
2.2.1|`-Pspark-2.2` | While use Spark 2.2.x and find any incompatible issue, you can specify `-Pspark2.2` to build kyuubi yourself
2.3.0|`-Pspark-2.3` | While use Spark 2.3.x and find any incompatible issue, you can specify `-Pspark2.3` to build kyuubi yourself

## Building a Runnable Distribution

To create a Kyuubi distribution like those distributed by [Kyuubi Release Page](https://github.com/yaooqinn/kyuubi/releases),
and that is laid out so as to be runnable, use `./build/dist` in the project root directory.

Example 1:
```bash
./build/dist --name custom-name --tgz
```

, which will build a Kyuubi distribution named `kyuubi-{version}-bin-custom-name.tar.gz` for you. 

Example 2:
```bash
./build/dist --tgz -Pspark-2.3
```
, which will build a Kyuubi distribution named `kyuubi-{version}-bin-spark-2.3.0.tar.gz` for you. 


For more information on usage, run `./build/dist --help`

## Running Tests
The following is an example of a command to run the tests:

```bash
./build/mvn clean test
```

With Maven, you can use the -DwildcardSuites flag to run individual Scala tests:

```bash
./build/mvn -Dtest=none -DwildcardSuites=yaooqinn.kyuubi.operation.OperationTypeSuite test
```

For more information about the ScalaTest Maven Plugin, refer to the [ScalaTest documentation](http://www.scalatest.org/user_guide/using_the_scalatest_maven_plugin).

## Additional Documentations

[Configuration Guide](https://yaooqinn.github.io/kyuubi/docs/configurations.html)  
[Authentication/Security Guide](https://yaooqinn.github.io/kyuubi/docs/authentication.html)  
[Kyuubi ACL Management Guide](https://yaooqinn.github.io/kyuubi/docs/authorization.html)  
[Kyuubi Architecture](https://yaooqinn.github.io/kyuubi/docs/architecture.html)  
[Home Page](https://yaooqinn.github.io/kyuubi/)
