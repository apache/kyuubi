Kyuubi Release Guide
===

This is based on the [release guide](https://beam.apache.org/contribute/release-guide/) of the Apache Beam project.

## Introduction
The Apache Kyuubi (Incubating) project periodically declares and publishes releases. A release is one or more packages
of the project artifact(s) that are approved for general public distribution and use. They may come with various
degrees of caveat regarding their perceived quality and potential for change, such as "alpha", "beta", "incubating",
"stable", etc.

The Kyuubi community treats releases with great importance. They are a public face of the project and most users
interact with the project only through the releases. Releases are signed off by the entire Kyuubi community in a
public vote.

Each release is executed by a Release Manager, who is selected among the Kyuubi committers. This document describes
the process that the Release Manager follows to perform a release. Any changes to this process should be discussed
and adopted on the [dev mailing list](mailto:dev@kyuubi.apache.org).

Please remember that publishing software has legal consequences. This guide complements the foundation-wide 
[Product Release Policy](http://www.apache.org/dev/release.html) and 
[Release Distribution Policy](http://www.apache.org/dev/release-distribution).

### Overview

![release process](../imgs/release/release-process.png)

The release process consists of several steps:

1. Decide to release
2. Prepare for the release
3. Investigate performance regressions
4. Create a release branch
5. Verify release branch
6. Build a release candidate
7. Vote on the release candidate
8. During vote process, run validation tests
9. If necessary, fix any issues and go back to step 3.
10. Finalize the release
11. Promote the release


