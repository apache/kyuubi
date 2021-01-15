# Kyuubi
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![](https://tokei.rs/b1/github/yaooqinn/kyuubi)](https://github.com/yaooqinn/kyuubi)
![GitHub top language](https://img.shields.io/github/languages/top/yaooqinn/kyuubi)
[![GitHub release](https://img.shields.io/github/release/yaooqinn/kyuubi.svg)](https://github.com/yaooqinn/kyuubi/releases)
[![codecov](https://codecov.io/gh/yaooqinn/kyuubi/branch/master/graph/badge.svg)](https://codecov.io/gh/yaooqinn/kyuubi)
[![HitCount](http://hits.dwyl.io/yaooqinn/kyuubi.svg)](http://hits.dwyl.io/yaooqinn/kyuubi)
[![Travis](https://travis-ci.org/yaooqinn/kyuubi.svg?branch=master)](https://travis-ci.org/yaooqinn/kyuubi)
![GitHub Workflow Status](https://img.shields.io/github/workflow/status/yaooqinn/kyuubi/Kyuubi/master?style=plastic)
[![Documentation Status](https://readthedocs.org/projects/kyuubi/badge/?version=latest)](https://kyuubi.readthedocs.io/en/latest/?badge=latest)
[![DepShield Badge](https://depshield.sonatype.org/badges/yaooqinn/kyuubi/depshield.svg)](https://depshield.github.io)

Kyuubi is a high-performance universal JDBC and SQL execution engine, built on top of [Apache Spark](http://spark.apache.org).
The goal of Kyuubi is to facilitate users to handle big data like ordinary data.

It provides a standardized JDBC interface with easy-to-use data access in big data scenarios.
End-users can focus on developing their own business systems and mining data value without having to be aware of the underlying big data platform (compute engines, storage services, metadata management, etc.).

Kyuubi relies on Apache Spark to provide high-performance data query capabilities,
and every improvement in the engine's capabilities can help Kyuubi's performance make a qualitative leap.
In addition, Kyuubi improves ad-hoc responsiveness through the engine caching,
and enhances concurrency through horizontal scaling and load balancing.
It provides complete authentication and authentication services to ensure data and metadata security.
It provides robust high availability and load balancing to help you guarantee the SLA commitment.
It provides a two-level elastic resource management architecture to effectively improve resource utilization while covering the performance and response requirements of all scenarios including interactive,
or batch processing and point queries, or full table scans.
It embraces Spark and builds an ecosystem on top of it,
which allows Kyuubi to quickly expand its existing ecosystem and introduce new features,
such as cloud-native support and `Data Lake/Lake House` support.

Kyuubi's vision is to build on top of Apache Spark and Data Lake technologies to unify the portal and become an ideal data lake management platform.
It can support data processing e.g. ETL, and analytics e.g. BI in a pure SQL way.
All workloads can be done on one platform, using one copy of data, with one SQL interface.

## Online Documentation

Since Kyuubi 1.0.0, the Kyuubi online documentation is hosted by [https://readthedocs.org/](https://readthedocs.org/).
You can find the specific version of Kyuubi documentation as listed below.

- [master/latest](https://kyuubi.readthedocs.io/en/latest/)
- [stable](https://kyuubi.readthedocs.io/en/stable/)
- [v1.0.2](https://kyuubi.readthedocs.io/en/v1.0.2/)
- [v1.0.1](https://kyuubi.readthedocs.io/en/v1.0.1/)
- [v1.0.0](https://kyuubi.readthedocs.io/en/v1.0.0/)

For 0.8 and earlier versions, please check the [project docs folder](https://github.com/yaooqinn/kyuubi/tree/branch-0.7/docs) directly.

## Quick Start

Ready? [Getting Started](https://kyuubi.readthedocs.io/en/latest/quick_start/quick_start.html) with Kyuubi.

## Contributing

All bits of help are welcome. You can make various types of contributions to Kyuubi, including the following but not limited to,

- Help new users in chat channel or share your success stories w/ us - [![Gitter](https://badges.gitter.im/kyuubi-on-spark/Lobby.svg)](https://gitter.im/kyuubi-on-spark/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)
- Improve Documentation - [![Documentation Status](https://readthedocs.org/projects/kyuubi/badge/?version=latest)](https://kyuubi.readthedocs.io/en/latest/?badge=latest)
- Test releases - [![GitHub release](https://img.shields.io/github/release/yaooqinn/kyuubi.svg)](https://github.com/yaooqinn/kyuubi/releases)
- Improve test coverage - [![codecov](https://codecov.io/gh/yaooqinn/kyuubi/branch/master/graph/badge.svg)](https://codecov.io/gh/yaooqinn/kyuubi)
- Report bugs and better help developers to reproduce
- Review changes
- Make a pull request
- Promote to others
- Click the star button if you like this project

## Aside

The project took its name from a character of a popular Japanese manga - `Naruto`.
The character is named `Kyuubi Kitsune/Kurama`, which is a nine-tailed fox in mythology.
`Kyuubi` spread the power and spirit of fire, which is used here to represent the powerful [Apache Spark](http://spark.apache.org).
It's nine tails stands for end-to end multi-tenancy support of this project.