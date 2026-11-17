# openEO processes based on Geotrellis

[![Status](https://img.shields.io/badge/Status-stable-green.svg)]()

This library implements openEO processes using the Geotrellis Spark API. 
It is used in combination with https://github.com/Open-EO/openeo-geopyspark-driver to provide a complete openEO backend.


## Running unit tests

### Test data

Many tests depend on the `testdata` git submodule ([geotrellis-testdata](https://github.com/Open-EO/geotrellis-testdata)),
which stores its binary files (`.tif`, `.nc`, `.jp2`, ...) with [Git LFS](https://git-lfs.com/).
Install Git LFS first (e.g. `sudo apt install git-lfs` or `brew install git-lfs`), then:

```bash
git lfs install                          # once per user, enables the LFS git hooks
git submodule update --init --recursive  # check out the testdata submodule
git -C testdata lfs pull                 # make sure the actual LFS files are downloaded
```

Alternatively, clone with `git clone --recurse-submodules ...`.
If test files contain small text pointers (`version https://git-lfs.github.com/spec/v1 ...`) instead of real data, 
Git LFS was not installed when checking out; run `git -C testdata lfs pull` to fix this.

When pulling new commits, keep the submodule in sync with `git submodule update --recursive`.

### Other requirements

While the most important tests can be executed anywhere, without requiring dependencies, there are some exceptions:

 * geotrellis-sentinelhub requires environment variables SENTINELHUB_CLIENT_ID and SENTINELHUB_CLIENT_SECRET to be set.
 * Some tests expect the Terrascope archive to be available under /data/MTDA

## Run full process graph integration test

A JUnit based test is available, [TestProcessGraphJson](https://github.com/Open-EO/openeo-geotrellis-extensions/blob/cef5ed3e44477a9f70daa897a71ed01a4b2d2968/openeo-geotrellis/src/test/scala/org/openeo/geotrellis/processgraph/TestProcessGraphJson.scala) that executes full process graphs, allowing to exactly reproduce end-to-end backend behavior.
The test will use Docker to pull in a working runtime environment.

For data access, it is possible to provide various types of credentials, for reading over http, or to work with locally mounted data.
There are also synthetic data providers, which are very much recommend to make tests data independent if possible.

Various process graphs in the [resources](https://github.com/Open-EO/openeo-geotrellis-extensions/blob/cef5ed3e44477a9f70daa897a71ed01a4b2d2968/openeo-geotrellis/src/test/resources/org/openeo/geotrellis/processgraph) folder provide examples.

### Debugging full process graph

For debugging, just run the test via your IDE in debug mode, it will start, but then wait for a remote debug session to be attached.
Use your IDE to connect to the remote debug port (5005 by default), and the process graph execution will continue, allowing you to set breakpoints.

## Releasing new major version
Setup clean local git repository with up to date develop and master branch.
Ensure git flow plugin is installed.

1. Ensure dev branch is in a proper state.
2. Freeze development on dev branch
3. Start release branch: `git flow release start 1.6.0-RC1`
4. Update develop branch versions: 
    * 'git checkout develop'
    * 'mvn versions:set -DnewVersion=2.0.0-SNAPSHOT'
    * 'git add pom.xml */pom.xml'
    * 'git commit && git push'
    *  Now development can safely continue.
5. Update dependencies in release branch to non-SNAPSHOT versions.
6. Make sure release branch builds without problems (failing tests) and commit.
7. Finish release branch: 'git flow release finish '1.6.0-RC1''
8. Push to git, in (VITO) Jenkins this will trigger a build. Release this build
