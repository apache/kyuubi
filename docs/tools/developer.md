<div align=center>

![](../imgs/kyuubi_logo_simple.png)

</div>

# Developer Tools

## Update Project Version

```bash

build/mvn versions:set -DgenerateBackupPoms=false
```

## Update Document Version

Whenever Project version updates, please also update the document version at `docs/conf.py` to target the upcoming release.

For example,

```python
release = '1.2.0'
```

## Update Dependency List

Kyuubi uses the `dev/dependencyList` file to indicates what upstream dependencies will actually go to the server-side classpath.

For Pull requests, a linter for dependency check will be automatically executed in GitHub Actions.


You can run `build/dependency.sh` locally first to detect the potential dependency change first.

If the changes look expected, run `build/dependency.sh --replace` to update `dev/dependencyList` in your Pull request.
