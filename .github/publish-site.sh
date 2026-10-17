#!/usr/bin/env bash
# Publish web/ to the lightweight `site` branch as a single ORPHAN commit.
#
# WHY (17 Oct: "the real fix is still the separate lightweight publish
# branch so deploys can run every couple of minutes without choking on
# the 1.8 GB repo"). A Render static deploy clones the branch it serves.
# main carries 1.8 GB of history because the sweep committed a 16 MB
# index.html thousands of times, so a clone — and every build — took
# tens of minutes and newer deploys cancelled older ones. This branch
# has NO history: one parentless commit, rewritten every deploy, holding
# only what the site serves, so Render clones ~30 MB and builds in
# seconds however heavy main gets.
#
# index.html and matchbank.json now live ONLY here, never on main, which
# also stops main from ever bloating again. web/live.json stays on main
# too, because the page polls it from main's raw URL between deploys.
set -eu
msg="${1:-site: $(date -u '+%Y-%m-%d %H:%M') UTC}"
idx="$(mktemp -u)"
trap 'rm -f "$idx"' EXIT
# Stage exactly what the static site serves — web/ plus the host config —
# into a throwaway index, so the working tree and HEAD are untouched.
GIT_INDEX_FILE="$idx" git add -f web render.yaml netlify.toml
tree="$(GIT_INDEX_FILE="$idx" git write-tree)"
# PARENTLESS: no -p, so the branch never accumulates history.
commit="$(git commit-tree "$tree" -m "$msg")"
for try in 1 2 3; do
  if git push -f origin "$commit:refs/heads/site"; then
    echo "published web/ to site ($commit) on try $try"; exit 0
  fi
  sleep $((try * 3))
done
echo "could not push site after 3 tries" >&2; exit 1
