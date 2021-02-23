<div align=center>

![](../imgs/kyuubi_logo_simple.png)

</div>

# Building a Runnable Distribution

To create a Kyuubi distribution like those distributed by [Kyuubi Release Page](https://github.com/yaooqinn/kyuubi/releases),
and that is laid out so as to be runnable, use `./build/dist` in the project root directory.

For more information on usage, run `./build/dist --help`

```logtalk
./build/dist - Tool for making binary distributions of Kyuubi Server

Usage:
+------------------------------------------------------+
| ./build/dist [--name] [--tgz] <maven build options>  |
+------------------------------------------------------+
name: -  custom binary name, using project version if undefined
tgz:  -  whether to make a whole bundled package
```

For instance,

```bash
./build/dist --name custom-name --tgz
```

This results a Kyuubi distribution named `kyuubi-{version}-bin-custom-name.tar.gz` for you.
