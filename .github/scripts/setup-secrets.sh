#!/usr/bin/env bash
# Interactively configures the secrets and variables used by .github/workflows/build.yml.
# Values are read locally with hidden input and sent straight to GitHub via the gh CLI.
# Docker images and Maven packages use GitHub Packages with the built-in GITHUB_TOKEN; no registry secrets needed.
set -euo pipefail

REPO="${REPO:-Open-EO/openeo-geotrellis-extensions}"

command -v gh >/dev/null || { echo "gh CLI not found, see https://cli.github.com" >&2; exit 1; }
gh auth status >/dev/null 2>&1 || gh auth login

secrets=(
  "AWS_ACCESS_KEY_ID|Jenkins credential 'SentinelHubBatchS3' (access key)"
  "AWS_SECRET_ACCESS_KEY|Jenkins credential 'SentinelHubBatchS3' (secret key)"
  "SENTINELHUB_CLIENT_ID|Jenkins credential 'SentinelHubGeodatadev' (username)"
  "SENTINELHUB_CLIENT_SECRET|Jenkins credential 'SentinelHubGeodatadev' (password)"
  "DOWNSTREAM_DISPATCH_TOKEN|(optional) PAT with access to Open-EO/openeo-geopyspark-driver"
)

variables=(
  "TEST_IMAGE|(optional) default ghcr.io/open-eo/openeo-geotrellis-test-image:20250921_1"
  "TEST_HELPER_IMAGE|(optional) default ghcr.io/open-eo/geotrellis_process_graph_test_helper:latest"
  "MAVEN_PACKAGES_URL|(optional) default https://maven.pkg.github.com/Open-EO/openeo-geotrellis-extensions"
  "RUNNER_LABEL|(optional) runner label, default ubuntu-latest"
)

existing_secrets=$(gh secret list -R "$REPO" --json name -q '.[].name' || true)
existing_vars=$(gh variable list -R "$REPO" --json name -q '.[].name' || true)

echo "Configuring secrets for $REPO. Leave a value empty to skip it."
for entry in "${secrets[@]}"; do
  name="${entry%%|*}"; desc="${entry#*|}"
  status=""; grep -qx "$name" <<<"$existing_secrets" && status=" [already set, empty keeps it]"
  read -rsp "$name - $desc$status: " value; echo
  if [[ -n "$value" ]]; then
    printf '%s' "$value" | gh secret set "$name" -R "$REPO"
  fi
  unset value
done

echo
echo "Configuring variables for $REPO. Leave a value empty to skip it."
for entry in "${variables[@]}"; do
  name="${entry%%|*}"; desc="${entry#*|}"
  status=""; grep -qx "$name" <<<"$existing_vars" && status=" [already set, empty keeps it]"
  read -rp "$name - $desc$status: " value
  if [[ -n "$value" ]]; then
    gh variable set "$name" -R "$REPO" --body "$value"
  fi
done

echo
read -rp "Create/update the 'release' environment (add required reviewers in the UI afterwards)? [y/N] " answer
if [[ "$answer" =~ ^[Yy]$ ]]; then
  gh api -X PUT "repos/$REPO/environments/release" >/dev/null
  echo "Environment 'release' ready: https://github.com/$REPO/settings/environments"
fi

echo "Done."
