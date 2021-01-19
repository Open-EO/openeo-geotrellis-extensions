# openEO processes based on Geotrellis

[![Status](https://img.shields.io/badge/Status-stable-green.svg)]()

This library implements openEO processes using the Geotrellis Spark API. 
It is used in combination with https://github.com/Open-EO/openeo-geopyspark-driver to provide a complete openEO backend.


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
