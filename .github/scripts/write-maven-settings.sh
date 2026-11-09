#!/usr/bin/env bash
# Writes a Maven settings file that resolves and deploys via GitHub Packages instead of VITO Artifactory.
# The VITO repositories declared in pom.xml (artifactory-release/artifactory-snapshot) are mirrored to GitHub Packages.
# Credentials are resolved from GITHUB_ACTOR/GITHUB_TOKEN at runtime.
set -euo pipefail

: "${MAVEN_PACKAGES_URL:?MAVEN_PACKAGES_URL must be set}"

mkdir -p "$HOME/.m2"
cat > "$HOME/.m2/ci-settings.xml" <<EOF
<settings>
  <servers>
    <server>
      <id>github</id>
      <username>\${env.GITHUB_ACTOR}</username>
      <password>\${env.GITHUB_TOKEN}</password>
    </server>
  </servers>
  <mirrors>
    <mirror>
      <id>github</id>
      <name>GitHub Packages</name>
      <mirrorOf>artifactory-release,artifactory-snapshot</mirrorOf>
      <url>${MAVEN_PACKAGES_URL}</url>
    </mirror>
  </mirrors>
  <profiles>
    <profile>
      <!-- Artifacts that VITO Artifactory proxied from public repositories not declared in the poms -->
      <id>public-repositories</id>
      <repositories>
        <repository>
          <id>osgeo</id>
          <url>https://repo.osgeo.org/repository/release/</url>
          <snapshots><enabled>false</enabled></snapshots>
        </repository>
        <repository>
          <id>snap-repo-public</id>
          <url>https://nexus.snap-ci.ovh/repository/snap-maven-public/</url>
          <snapshots><enabled>false</enabled></snapshots>
        </repository>
      </repositories>
    </profile>
  </profiles>
  <activeProfiles>
    <activeProfile>public-repositories</activeProfile>
  </activeProfiles>
</settings>
EOF
