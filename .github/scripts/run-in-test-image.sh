#!/usr/bin/env bash
# Runs the given command inside the test image, like Jenkins' testImage.inside(...).
set -euo pipefail

: "${TEST_IMAGE:?TEST_IMAGE must be set}"

mkdir -p "$HOME/.m2"

status=0
docker run --rm \
  -u root \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$HOME/.m2:/root/.m2" \
  -v "$GITHUB_WORKSPACE:$GITHUB_WORKSPACE" \
  -w "$GITHUB_WORKSPACE" \
  -e SPARK_LOCAL_IP=127.0.0.1 \
  -e JAVA_HOME=/usr/lib/jvm/java-21-openjdk \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
  -e SENTINELHUB_CLIENT_ID -e SENTINELHUB_CLIENT_SECRET \
  -e ARTIFACTORY_USERNAME -e ARTIFACTORY_PASSWORD \
  -e SAS_ARTIFACTORY_USERNAME -e SAS_ARTIFACTORY_PASSWORD \
  --entrypoint "" \
  "$TEST_IMAGE" \
  bash -c 'git config --global --add safe.directory "$PWD" && exec "$@"' bash "$@" || status=$?

# Restore ownership of files created by root inside the container.
docker run --rm -u root --entrypoint "" \
  -v "$HOME/.m2:/root/.m2" -v "$GITHUB_WORKSPACE:$GITHUB_WORKSPACE" \
  "$TEST_IMAGE" chown -R "$(id -u):$(id -g)" /root/.m2 "$GITHUB_WORKSPACE" || true

exit "$status"
