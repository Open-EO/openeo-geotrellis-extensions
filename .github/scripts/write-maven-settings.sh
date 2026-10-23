#!/usr/bin/env bash
# Writes a Maven settings file with Artifactory credentials resolved from environment variables at runtime.
set -euo pipefail

mkdir -p "$HOME/.m2"
cat > "$HOME/.m2/ci-settings.xml" <<'EOF'
<settings>
  <servers>
    <server>
      <id>vito-artifactory</id>
      <username>${env.ARTIFACTORY_USERNAME}</username>
      <password>${env.ARTIFACTORY_PASSWORD}</password>
    </server>
    <server>
      <id>sas-artifactory</id>
      <username>${env.SAS_ARTIFACTORY_USERNAME}</username>
      <password>${env.SAS_ARTIFACTORY_PASSWORD}</password>
    </server>
  </servers>
</settings>
EOF
