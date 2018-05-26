# Building Kyuui

## Building Kyuubi with Apache Maven
**Kyuubi** server is built based on [Apache Maven](http://maven.apache.org),

```bash
./build/mvn clean package -DskipTests
```

Running the code above in the Kyuubi project root directory is all we need to build a runnable Kyuubi server.

## Building a Runnable Distribution

To create a Kyuubi distribution like those distributed by [Kyuubi Release Page](https://github.com/yaooqinn/kyuubi/releases),
and that is laid out so as to be runnable, use `./build/dist` in the project root directory.

Example:
```bash
./build/dist --name custom-name --tgz
```

This will build a Kyuubi distribution name `kyuubi-{version}-bin-custom-name.tar.gz`. For more information on usage,
run `./build/dist --help`

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
[Kyuubi Architecture](https://yaooqinn.github.io/kyuubi/docs/architecture.html)  
[Home Page](https://yaooqinn.github.io/kyuubi/)
