#/bin/bash
PUBLISH_WHITELIST="master foo"

# We only publish on commits to whitelisted branches, not PRs against them or tagging events
# This isn't a pull request build
[[ "${TRAVIS_PULL_REQUEST}" == "false" ]] \
  && [[ "${TRAVIS_TAG}" == "" ]] \
  && [[ ${PUBLISH_WHITELIST} =~ ${TRAVIS_BRANCH} ]] \
  && travis/ci-tag && mvn versions:set -DnewVersion=$(git describe) \
  && mvn deploy --settings travis/bintray-settings.xml
  && mvn versions:revert ;
