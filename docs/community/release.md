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

![release process](../imgs/release/release-process.png)

The release process consists of several steps:

1. Decide to release
2. Prepare for the release
3. Build a release candidate
4. Vote on the release candidate
5. If necessary, fix any issues and go back to step 3.
6. Finalize the release
7. Promote the release

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

#### Subversion

`svn` is required for Apache release, please refer to
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
pub   ed25519/ED4E2E5B 2021-07-02 [SC]
      2A145AF50886431A8FA86089F07E6C29ED4E2E5B
uid         [ultimate] Cheng Pan <chengpan@apache.org>
sub   cv25519/C7207C04 2021-07-02 [E]
```
Here, the key ID is the 8-digit hex string in the pub line: ED4E2E5B.

The last step is to update the KEYS file with your code signing key 
https://www.apache.org/dev/openpgp.html#export-public-key

```shell
svn checkout --depth=files "https://dist.apache.org/repos/dist/dev/incubator/kyuubi" svn-kyuubi
... edit svn-kyuubi/KEYS file
svn commit --username "${ASF_USERNAME}" --password "${ASF_PASSWORD}" --message "Update KEYS"
```


