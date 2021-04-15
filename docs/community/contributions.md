<script async defer src="https://buttons.github.io/buttons.js"></script>

# Contribution Guidelines

**Kyuubi** is an [Apache License v2.0](https://github.com/yaooqinn/kyuubi/blob/master/LICENSE) open source software.

Contributing to Kyuubi including source code, documents, tests e.t.c. means that you agree to the Apache License v2.0.

- Better to search the issue history first before reporting an <a class="github-button" href="https://github.com/yaooqinn/kyuubi/issues" data-color-scheme="no-preference: light; light: dark; dark: light;" data-icon="octicon-issue-opened" data-show-count="true" aria-label="Issue yaooqinn/kyuubi on GitHub">Issue</a>
- Better to create an <a class="github-button" href="https://github.com/yaooqinn/kyuubi/issues" data-color-scheme="no-preference: light; light: dark; dark: light;" data-icon="octicon-issue-opened" data-show-count="true" aria-label="Issue yaooqinn/kyuubi on GitHub">Issue</a> to describe the feature or bug first before make a pull request.
- Better to use English for world widely understanding.
- Ask us anything 

Before you start, please read the [Code of Conduct](http://www.apache.org/foundation/policies/conduct.html) carefully, familiarize yourself with it and refer to it whenever you need it.

## Prepare github environment
If you are new to submit a Pull Request, the follow steps are helpful to you.

1. An available [git](https://git-scm.com/downloads), you can run `git version` if you not sure you have
2. Fork [Kyuubi](https://github.com/NetEase/kyuubi) on github, now you have cloned Kyuubi repo 
3. Clone your Kyuubi repo with cmd `git clone https://github.com/${yourname}/kyuubi.git`
4. Create a new branch with cmd `git checkout -b test-branch`
5. Modify the code you want
6. Commit and push code to your Kyuubi repo with commd `git commit -am "comment"; git push test-branch test-branch`
7. Back to [Kyuubi](https://github.com/NetEase/kyuubi), you can a banner about `new pull request`
8. Now we can create a pull request

## Creating a Pull Request

When creating a Pull Request, you will automatically get the template below.

Fulfilling it thoroughly can improve the speed of the review process.

```
<!--
Thanks for sending a pull request!

Here are some tips for you:
  1. If this is your first time, please read our contributor guidelines:
     https://kyuubi.readthedocs.io/en/latest/community/contributions.html
-->

### _Which issue are you going to fix?_
<!--
Replace ${ID} below with the actual issue id from
https://github.com/yaooqinn/kyuubi/issues,
so that the issue will be linked and automatically closed after merging
-->

Fixes #${ID}

### _Why are the changes needed?_
<!--
Please clarify why the changes are needed. For instance,
  1. If you add a feature, you can talk about the user case of it.
  2. If you fix a bug, you can clarify why it is a bug.
-->


### _How was this patch tested?_
- [ ] Add some test cases that check the changes thoroughly including negative and positive cases if possible

- [ ] Add screenshots for manual tests if appropriate

- [ ] [Run test](https://kyuubi.readthedocs.io/en/latest/tools/testing.html#running-tests) locally before make a pull request

```
