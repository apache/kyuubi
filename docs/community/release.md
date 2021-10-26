<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

Kyuubi Release Guide
===

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
[Product Release Policy](https://www.apache.org/dev/release.html) and 
[Release Distribution Policy](https://www.apache.org/dev/release-distribution).

### Overview

The release process consists of several steps:

1. Decide to release
2. Prepare for the release
3. Cut branch iff for __major__ release
4. Build a release candidate
5. Vote on the release candidate
6. If necessary, fix any issues and go back to step 3.
7. Finalize the release
8. Promote the release

## Decide to release

Deciding to release and selecting a Release Manager is the first step of the release process. This is a consensus-based
decision of the entire community.

Anybody can propose a release on the [dev mailing list](mailto:dev@kyuubi.apache.org), giving a solid argument and
nominating a committer as the Release Manager (including themselves). There’s no formal process, no vote requirements,
and no timing requirements. Any objections should be resolved by consensus before starting the release.

In general, the community prefers to have a rotating set of 1-2 Release Managers. Keeping a small core set of managers
allows enough people to build expertise in this area and improve processes over time, without Release Managers needing
to re-learn the processes for each release. That said, if you are a committer interested in serving the community in
this way, please reach out to the community on the [dev mailing list](mailto:dev@kyuubi.apache.org).

### Checklist to proceed to the next step

1. Community agrees to release
2. Community selects a Release Manager

## Prepare for the release

Before your first release, you should perform one-time configuration steps. This will set up your security keys for
signing the release and access to various release repositories.

### One-time setup instructions

#### ASF authentication

The environments `ASF_USERNAME` and `ASF_PASSWORD` have been used in several places and several times in the release
process, you can either one-time set up them in `~/.bashrc` or `~/.zshrc`, or export them in terminal everytime.

```shell
export ASF_USERNAME=<your apache username>
export ASF_PASSWORD=<your apache password>
```

#### Java Home
An available environment variable `JAVA_HOME`, you can do `echo $JAVA_HOME` to check it.
Note that, the Java version should be 8.

#### Subversion

Besides on `git`, `svn` is also required for Apache release, please refer to
https://www.apache.org/dev/version-control.html#https-svn for details.

#### GPG Key

You need to have a GPG key to sign the release artifacts. Please be aware of the ASF-wide
[release signing guidelines](https://www.apache.org/dev/release-signing.html). If you don’t have a GPG key associated
with your Apache account, please create one according to the guidelines.

Determine your Apache GPG Key and Key ID, as follows:
```shell
gpg --list-keys --keyid-format SHORT
```

This will list your GPG keys. One of these should reflect your Apache account, for example:
```shell
pub   rsa4096 2021-08-30 [SC]
      8FC8075E1FDC303276C676EE8001952629BCC75D
uid           [ultimate] Cheng Pan <chengpan@apache.org>
sub   rsa4096 2021-08-30 [E]
```
Here, the key ID is the 8-digit hex string in the pub line: `29BCC75D`.

To export the PGP public key, using:
```shell
gpg --armor --export 29BCC75D
```

If you have more than one gpg key, you can specify the default key as following:
```
echo 'default-key <key-fpr>' > ~/.gnupg/gpg.conf
```

The last step is to update the KEYS file with your code signing key 
https://www.apache.org/dev/openpgp.html#export-public-key

```shell
svn checkout --depth=files "https://dist.apache.org/repos/dist/dev/incubator/kyuubi" work/svn-kyuubi
(gpg --list-sigs "${ASF_USERNAME}@apache.org" && gpg --export --armor "${ASF_USERNAME}@apache.org") >> KEYS
svn commit --username "${ASF_USERNAME}" --password "${ASF_PASSWORD}" --message "Update KEYS" work/svn-kyuubi
```

## Cut branch iff for major release

Kyuubi use version pattern `{MAJOR_VERSION}.{MINOR_VERSION}.{PATCH_VERSION}[-{OPTIONAL_SUFFIX}]`, e.g. `1.3.0-incubating`.
__Major Release__ means `MAJOR_VERSION` or `MINOR_VERSION` changed, and __Patch Release__ means `PATCH_VERSION` changed.

The main step towards preparing a major release is to create a release branch. This is done via standard Git branching
mechanism and should be announced to the community once the branch is created.

The release branch pattern is `branch-{MAJOR_VERSION}.{MINOR_VERSION}`, e.g. `branch-1.3`.

After cutting release branch, don't forget bump version in `master` branch.

## Build a release candidate

1. Set environment variables.

```shell
export RELEASE_VERSION=<release version, e.g. 1.3.0-incubating>
export RELEASE_RC_NO=<RC number, e.g. 0>
```

2. Bump version.

```shell
build/mvn versions:set -DgenerateBackupPoms=false \
  -DnewVersion="${RELEASE_VERSION}" \
  -Pkubernetes,kyuubi-extension-spark-3-1,spark-block-cleaner,tpcds

git commit -am "[RELEASE] Bump ${RELEASE_VERSION}"
```

3. Create a git tag for the release candidate.

The tag pattern is `v${RELEASE_VERSION}-rc${RELEASE_RC_NO}`, e.g. `v1.3.0-incubating-rc0`

4. Package the release binaries & sources, and upload them to the Apache staging SVN repo. Publish jars to the Apache
staging Maven repo.

```shell
build/release/release.sh publish
```

## Vote on the release candidate

The release voting takes place on the Apache Kyuubi (Incubating) developers list (the (P)PMC is voting).

- If possible, attach a draft of the release notes with the email.
- Recommend represent voting closing time in UTC format.
- Make sure the email is in text format and the links are correct

Once the vote is done, you should also send out a summary email with the totals, with a subject that looks
something like __[VOTE][RESULT] ....__

## Finalize the Release

__Be Careful!__

__THIS STEP IS IRREVERSIBLE so make sure you selected the correct staging repository.__
__Once you move the artifacts into the release folder, they cannot be removed.__

After the vote passes, to upload the binaries to Apache mirrors, you move the binaries from dev directory (this should
be where they are voted) to release directory. This "moving" is the only way you can add stuff to the actual release
directory. (Note: only (P)PMC members can move to release directory)

Move the sub-directory in "dev" to the corresponding directory in "release". If you've added your signing key to the
KEYS file, also update the release copy.

```shell
build/release/release.sh finalize
```

Verify that the resources are present in https://www.apache.org/dist/incubator/kyuubi/. It may take a while for them
to be visible. This will be mirrored throughout the Apache network.

For Maven Central Repository, you can Release from the [Apache Nexus Repository Manager](https://repository.apache.org/).
Log in, open Staging Repositories, find the one voted on, select and click Release and confirm. If successful, it should
show up under https://repository.apache.org/content/repositories/releases/org/apache/kyuubi/ and the same under 
https://repository.apache.org/content/groups/maven-staging-group/org/apache/kyuubi/ (look for the correct release version).
After some time this will be sync’d to [Maven Central](https://search.maven.org/) automatically.

## Promote the release

### Update Website

Fork and clone [Apache Kyuubi website](https://github.com/apache/incubator-kyuubi-website)

1. Add a new markdown file in `src/news/`
2. Add a new markdown file in `src/release/`

### Create an Announcement

Once everything is working create an announcement on the website and then send an e-mail to the mailing list.
The mailing list includes: `announce@apache.org`, `dev@kyuubi.apache.org`, `user@spark.apache.org`.

Enjoy an adult beverage of your choice, and congratulations on making a Kyuubi release.
